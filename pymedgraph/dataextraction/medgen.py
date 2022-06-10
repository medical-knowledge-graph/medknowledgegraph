import pandas as pd
from pymedgraph.io.fetch_ncbi import NCBIFetcher
from pymedgraph.dataextraction.parser import parse_medgen
from pymedgraph.dataextraction.basepipe import BasePipe, PipeOutput, NodeTable

class MedGenPipe(BasePipe):
    """
    Class to make request to NCBI MedGen database based on CUI
    """
    def __init__(self, ncbi_fetcher: NCBIFetcher,depends_on=None):
        super().__init__('MedGenPipe', depends_on=depends_on)

        self.fetcher = ncbi_fetcher

    @property
    def fetcher(self):
        return self._fetcher

    @fetcher.setter
    def fetcher(self, f):
        if not isinstance(f, NCBIFetcher):
            AttributeError('MedGenPipe.fetcher must be an pymedgraph.io.fetch_ncbi.NCBIFetcher instance.')
        self._fetcher = f

    def _run_pipe(self, df_entities: pd.DataFrame, df_links: pd.DataFrame,
                  snomed=False, clinical_features=False) -> PipeOutput:

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

    def _select_cui(self, df_entity: pd.DataFrame, df_links: pd.DataFrame, n_=5, cui_n=3) -> list:
        """ Filter for N CUI ids """
        cuis = list()
        # select n most found entities
        entities = df_entity[df_entity[self.NODEL_LABEL_COL] == 'DISEASE']['text'].value_counts()[:n_].index.tolist()
        # get n cui ids for each entity
        for ent in entities:
            links = df_links[
                        (df_links[self.SOURCE_COL] == ent) & (df_links['kb_score'] > 0.9)
                        ].sort_values(by='kb_score', ascending=False)['CUI'].values.tolist()[:cui_n]
            if links:
                cuis += links
        return cuis

    def _build_gene_df(self, summaries: dict) -> pd.DataFrame:
        data = list()
        for k, summary in summaries.items():
            for gene in summary['genes']:
                data.append((summary['cui'], gene))
        df = pd.DataFrame(data, columns=[self.SOURCE_COL, 'gene'])
        df[self.NODEL_LABEL_COL] = 'Gene'
        return df

    def _build_snomed_df(self, summaries: dict) -> pd.DataFrame:
        data = list()
        for k, summary in summaries.items():
            for id_, concept in summary['snomed'].items():
                data.append((summary['cui'], id_, concept['text'], concept['SCUI'], concept['SAB']))
        df = pd.DataFrame(data, columns=[self.SOURCE_COL, 'SAUI', 'snomed_text', 'SCUI','SAB'])
        df[self.NODEL_LABEL_COL] = 'SnomedConcept'
        return df

    def _build_clinical_features_df(self, summaries: dict) -> pd.DataFrame:
        data = list()
        for k, summary in summaries.items():
            for cui, feature in summary['clinical_features'].items():
                data.append((summary['cui'], cui, feature['type'], feature['name'], feature['definition']))
        df = pd.DataFrame(
            data,
            columns=[self.SOURCE_COL, 'CUI', 'type', 'name', 'definition'])
        df[self.NODEL_LABEL_COL] = 'ClinicalFeature'
        return df
