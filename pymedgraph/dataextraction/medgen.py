import pandas as pd
import json

from pymedgraph.input.fetch_ncbi import NCBIFetcher
from pymedgraph.dataextraction.parser import parse_medgen
from pymedgraph.dataextraction.basepipe import BasePipe, PipeOutput, NodeTable


class MedGenPipe(BasePipe):
    """
    Class to make request to NCBI MedGen database based on CUI id`s.
    The idea is to look for genes in the most found UMLS concepts in abstracts for a given search term.
    Therefore, we collect the ID´s of the most prominent UMLS concepts -> CUI`s and make a request to the MedGen
    database, which returns a ton of useful information. Since the database returns an XML we first have to parse it.
    For this we use the `pymedgraph.dataextraction.parser.parse_medgen()` method.
    We are always looking for genes, but the pipeline can also search for Snomed concepts and clinical features if the
    flag is set.

    As every other `pymedgraph.dataextraction.basepipe.BaspePipe` subclass the main method is the
    `MedGenPipe._run_pipe()` method, which will be executed from the manager for an API request and returns an
    `pymedgraph.dataextraction.basepipe.PipeOutput` object containing NodeTables.

    Clinical Features: A record about a condition may include a section describing the features of the condition.
    These data are provided from either the Human Phenotype Ontology (HPO) or OMIM. The first five features are
    displayed, with an option to view the full list. https://www.ncbi.nlm.nih.gov/medgen/docs/help/#clinical-features
    """
    def __init__(self, ncbi_fetcher: NCBIFetcher, depends_on=None, **kwargs):
        """
        Init class.
        kwargs can contain `medgen_list`, `medgen_list_path` and `max_concepts`.
        If `medgen_list` flag is True, then the class tries to read an existing file, which contains a list of
        CUI id´s. This ID`s are coming from MedGen Gene relationship table and states only concept Id`s with gene
        information. The list is transformed to a set to have quicker look ups and the class sets
        `MedGenPipe.use_medgen_set` to True. This has influence on the `MedGenPipe._select_cui()` method.
        """
        super().__init__('MedGenPipe', depends_on=depends_on)

        self.fetcher = ncbi_fetcher

        # parse kwargs
        if 'medgen_list' in kwargs:
            if kwargs['medgen_list']:
                # look for file
                if 'medgen_list_path' in kwargs:
                    try:
                        self.medgen_gene_set = self._read_medgen_list(kwargs['medgen_list_path'])
                        self.use_medgen_set = True
                    except:
                        print(f'WARNING: cannot read medgen gene list from {kwargs["medgen_list_path"]}')
                        self.use_medgen_set = False
                else:
                    self.use_medgen_set = False
        else:
            self.use_medgen_set = False
        if 'max_concepts' in kwargs:
            self.max_concepts = kwargs['max_concepts']
        else:
            self.max_concepts = 50

    @property
    def fetcher(self):
        return self._fetcher

    @fetcher.setter
    def fetcher(self, f):
        if not isinstance(f, NCBIFetcher):
            AttributeError('MedGenPipe.fetcher must be an pymedgraph.input.fetch_ncbi.NCBIFetcher instance.')
        self._fetcher = f

    def _run_pipe(self, df_entities: pd.DataFrame, df_links: pd.DataFrame,
                  snomed: bool = False, clinical_features:bool = False) -> PipeOutput:
        """
        This method is the main class method and proceeds in the following steps:
        1. collects most frequent UMLS concept IDS (-> CUI`s) from `df_entities` and `df_links`
        2. Makes a MedGen database request with the CUI´s
        3. parse XML response, with `pymedgraph.dataextraction.parser.parse_medgen()`
        4. Builds `pymedgraph.dataextraction.basepipe.NodeTable` objects for:
            - Genes: ALWAYS
            - SnomedConcept: only if `snomed` flag is True
            - Clinical Features: only if `clinical_features` flag is True

        :param df_entities: pd.DataFrame - contains entities, used to select n most mentioned entities
        :param df_links: pd.DataFrame - umls concepts, used to select concepts CUI for MedGen fetch
        :param snomed: bool - flag, if SnomedConcepts shall be extracted from MedGen response
        :param clinical_features: bool - flag, if ClinicalFeatures shall be extracted from MedGen response
        :return: `pymedgraph.dataextraction.basepipe.PipeOutput` object
        """
        output = PipeOutput(self.name)

        # select IDs
        cuis = self._select_cui(df_entities, df_links)
        # get data
        medgenrecords = self.fetcher.get_medgen_summaries(cuis)
        # parse xml records
        medgen_summaries = parse_medgen(medgenrecords, snomed=snomed, clinical_features=clinical_features)

        # build DataFrames
        df_gene = self._build_gene_df(medgen_summaries)
        output.add(NodeTable(
            name='Genes',
            df=df_gene,
            source_node='UMLS',
            source_node_attr='CUI',
            source_col=self.SOURCE_COL,
            node_label='Gene',
            id_attribute='gene',
            attribute_cols=''
        ))
        if snomed:
            df_snomed = self._build_snomed_df(medgen_summaries)
            output.add(NodeTable(
                name='Snomed',
                df=df_snomed,
                source_node='UMLS',
                source_node_attr='CUI',
                source_col=self.SOURCE_COL,
                node_label='SnomedConcept',
                id_attribute='SAUI',
                attribute_cols=['snomed_text', 'SCUI', 'SAB']
            ))
        if clinical_features:
            df_cf = self._build_clinical_features_df(medgen_summaries)
            output.add(NodeTable(
                name='ClinicalFeats',
                df=df_cf,
                source_node='UMLS',
                source_node_attr='CUI',
                source_col=self.SOURCE_COL,
                node_label='ClinicalFeature',
                id_attribute='CUI',
                attribute_cols=['type', 'name', 'definition']
            ))

        return output

    def _select_cui(self, df_entity: pd.DataFrame, df_links: pd.DataFrame, n_=15, cui_n=4) -> list:
        """
        Filter for n CUI ids. Filtering is done by selecting most popular entities found in paper and then selecting
        `cui_n` UMLS concepts to request MedGen.

        :param df_entity: pd.DataFrame - contains entities, used to select n most mentioned entities
        :param df_links: pd.DataFrame - umls concepts, used to select concepts CUI for MedGen fetch
        :param n_: int - number of top n most entities, which will be selected for CUI selection
        :param cui_n: int - of `n_` entities cui_n are selected for actual MedGen request
        :returns: list - containing CUI´s to make MedGen request
        """

        if self.use_medgen_set:
            cui_links = df_links[df_links['kb_score']>0.85]['CUI'].unique()
            cuis = [i for i in cui_links if i in self.medgen_gene_set]
        else:
            cuis = list()
            # select n most found entities
            entities = df_entity[df_entity[self.NODEL_LABEL_COL] == 'DISEASE']['text'].value_counts()[:n_].index.tolist()
            # get n cui ids for each entity
            for ent in entities:
                links = df_links[
                            (df_links[self.SOURCE_COL] == ent) & (df_links['kb_score'] > 0.85)
                            ].sort_values(by='kb_score', ascending=False)['CUI'].values.tolist()[:cui_n]
                if links:
                    cuis += links

        return list(set(cuis))

    def _build_gene_df(self, summaries: dict) -> pd.DataFrame:
        """
        This method builds a Gene pd.DataFrame with MedGen `summaries` parsed response.
        :param summaries: dict - returned by `pymedgraph.dataextraction.parser.parse_medgen()` containing summary of
        MedGen responses.
        :return: pd.DataFrame - containing Gene names and CUI
        """
        data = list()
        for k, summary in summaries.items():
            for gene in summary['genes']:
                data.append((summary['cui'], gene))
        df = pd.DataFrame(data, columns=[self.SOURCE_COL, 'gene'])
        df[self.NODEL_LABEL_COL] = 'Gene'
        return df

    def _build_snomed_df(self, summaries: dict) -> pd.DataFrame:
        """
        This method builds a Snomed pd.DataFrame from MedGen summary in `summaries`.
        Snomed CT is a core clinical healthcare terminology.
        :param summaries: dict - returned by `pymedgraph.dataextraction.parser.parse_medgen()` containing summary of
        MedGen responses.
        :return: pd.DataFrame - containing Snomed CT concept values, such das concept id and text
        """
        data = list()
        for k, summary in summaries.items():
            for id_, concept in summary['snomed'].items():
                data.append((summary['cui'], id_, concept['text'], concept['SCUI'], concept['SAB']))
        df = pd.DataFrame(data, columns=[self.SOURCE_COL, 'SAUI', 'snomed_text', 'SCUI','SAB'])
        df[self.NODEL_LABEL_COL] = 'SnomedConcept'
        return df

    def _build_clinical_features_df(self, summaries: dict) -> pd.DataFrame:
        """
        This method builds a ClinicalFeature pd.DataFrame from each MedGen summary in `summaries`.
        Clinical Features are describing features of the condition, such as Headaches
        :param summaries: dict - returned by `pymedgraph.dataextraction.parser.parse_medgen()` containing summary of
        MedGen responses.
        :return: pd.DataFrame - containing CUI and clinical feature values, such as type, name and definition
        """
        data = list()
        for k, summary in summaries.items():
            for cui, feature in summary['clinical_features'].items():
                data.append((summary['cui'], cui, feature['type'], feature['name'], feature['definition']))
        df = pd.DataFrame(
            data,
            columns=[self.SOURCE_COL, 'CUI', 'type', 'name', 'definition'])
        df[self.NODEL_LABEL_COL] = 'ClinicalFeature'
        return df

    @staticmethod
    def _read_medgen_list(file_path: str) -> set:
        # read file and transform list to set
        with open(file_path, 'r') as fh:
            medgen_genes = set([line[:-1] for line in fh.readlines()])
        return medgen_genes
