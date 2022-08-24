import pytest
import json
import pandas as pd


@pytest.fixture
def nodetable_df():
    """
        NodeTable(
            name='pubmedPaper',
            df=df,
            source_node='SearchTerm',
            source_node_attr='label',
            source_col=self.SOURCE_COL,
            node_label=node_label,
            id_attribute='pubmedID',
            attribute_cols=self._attribute_columns

            |source |node label | node attribute 1  | ... |node attribute X  |
            |---    |---        |---                | ... |---               |
    """
    return pd.DataFrame({
        'source': ['C0031485', 'C0268465'],
        'node_label': ['Gene', 'Gene'],
        'gene': ['PAH', 'QDPR']
    })


@pytest.fixture
def request_json():
    return json.dumps({
        'disease': 'phenylketonurie',
        'n_articles': 100,  # number of articles to be fetched
        'pipelines': {
            'pubmed': {
                'run': True,  # this database is required and must be set
                'meshTerms': True  # flag if MeSH terms shall be extracted
            },
            'ner': {
                'run': True,
                'entityLinks': True
                # flag if links to UMLS knowledge base shall be extracted --> required for farther pipelines
            },
            'medGen': {
                'run': True,
                'Snomed': True,  # flag if SnomedConcepts are extracted
                'clinicalFeatures': False  # flag for clinical Features
            },
            'uniProt': {'run': False}
        }
    })