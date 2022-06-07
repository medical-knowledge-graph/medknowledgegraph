import pandas as pd
from pymedgraph.io.fetch_ncbi import NCBIFetcher
from pymedgraph.dataextraction.parser import parse_medgen
from pymedgraph.dataextraction.basepipe import BasePipe

class MedGenPipe(BasePipe):
    """
    Class to make request to NCBI MedGen database based on CUI
    """
    def __init__(self, ncbi_fetcher: NCBIFetcher,depends_on=None):
        super().__init__('MedGenPipe', depends_on=depends_on)

        self.fetcher = ncbi_fetcher

        self.columns  = self._set_column_names(
            ['gene', 'SAUI', 'snomed_text', 'SCUI', 'SAB', 'CUI', 'type', 'name', 'definition']
        )

    def _run_pipe(self, df_entities: pd.DataFrame, df_links: pd.DataFrame,
                  snomed=False, clinical_features=False) -> list:
        # select IDs
        cuis = self._select_cui(df_entities, df_links)
        # get data
        medgenrecords = self.fetcher.get_medgen_summaries(cuis)
        # parse xml records
        medgen_summaries = parse_medgen(medgenrecords, snomed=snomed, clinical_features=clinical_features)

        # build DataFrames
        df_gene = self._build_gene_df(medgen_summaries)
        output = [df_gene]
        if snomed:
            df_snomed = self._build_snomed_df(medgen_summaries)
            output.append(df_snomed)
        if clinical_features:
            df_cf = self._build_clinical_features_df(medgen_summaries)
            output.append(df_cf)

        return output

    def _select_cui(self, df_entity: pd.DataFrame, df_links: pd.DataFrame, n_=5, cui_n=3) -> list:
        """ Filter for N CUI ids """
        cuis = list()
        # select n most found entities
        entities = df_entity[df_entity[self.NODEL_LABEL_COL] == 'DISEASE']['$attr$text'].value_counts()[:n_].index.tolist()
        # get n cui ids for each entity
        for ent in entities:
            links = df_links[
                        (df_links[self.SOURCE_COL] == ent) & (df_links['kb_score'] > 0.9)
                        ].sort_values(by='kb_score', ascending=False)['$attr$CUI'].values.tolist()[:cui_n]
            if links:
                cuis += links
        return cuis

    def _build_gene_df(self, summaries: dict) -> pd.DataFrame:
        data = list()
        for k, summary in summaries.items():
            for gene in summary['genes']:
                data.append((summary['cui'], gene))
        df = pd.DataFrame(data, columns=[self.SOURCE_COL, self.columns['gene']])
        df[self.NODEL_LABEL_COL] = 'Gene'
        return df

    def _build_snomed_df(self, summaries: dict) -> pd.DataFrame:
        data = list()
        for k, summary in summaries.items():
            for id_, concept in summary['snomed'].items():
                data.append((summary['cui'], id_, concept['text'], concept['SCUI'], concept['SAB']))
        df = pd.DataFrame(data, columns=[
            self.SOURCE_COL, self.columns['SAUI'], self.columns['snomed_text'], self.columns['SCUI'],
            self.columns['SAB']])
        df[self.NODEL_LABEL_COL] = 'SnomedConcept'
        return df

    def _build_clinical_features_df(self, summaries: dict) -> pd.DataFrame:
        data = list()
        for k, summary in summaries.items():
            for cui, feature in summary['clinical_features'].items():
                data.append((summary['cui'], cui, feature['type'], feature['name'], feature['definition']))
        df = pd.DataFrame(
            data,
            columns=[self.SOURCE_COL, self.columns['CUI'], self.columns['type'], self.columns['name'],
                     self.columns['definition']])
        df[self.NODEL_LABEL_COL] = 'ClinicalFeature'
        return df
