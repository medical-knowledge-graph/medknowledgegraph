import pytest
import json


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