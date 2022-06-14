import os
import json

from pymedgraph.io.fetch_ncbi import NCBIFetcher
from pymedgraph.dataextraction import (
    get_mash_terms,
    get_pubmed_id,
    get_keywords,
    get_pubmed_title
)
from pymedgraph.dataextraction import StandardPubMedPipe, NERPipe, MedGenPipe


class ExtractionPipe(object):
    def __init__(self, name, method):
        self.name = name
        self.method = method

    def run_pipe(self, arguments):
        return self.method(arguments)


class MedGraphManager(object):
    """ Class to manage requests and graph build"""

    DISEASE = 'disease'
    REQUIRED_REQUEST_ARGS = [DISEASE]

    def __init__(self, config_path: str = 'localconfig.json'):
        # read config
        self.cfg = self._read_config(config_path)
        # init fetcher for api requests to pubmed
        self.ncbi_fetcher = NCBIFetcher(
            email=self.cfg['NCBI']['email'],
            tool_name=self.cfg['NCBI']['tool_name'],
            max_articles=self.cfg['NCBI']['max_articles']
        )

        # init pipelines
        self.pubmed_pipe = StandardPubMedPipe()
        self.ner_pipe = NERPipe(nlp_model='en_ner_bc5cdr_md', entity_linker='umls', depends_on='StandardPubMedPipe')
        self.medgen_pipe = MedGenPipe(self.ncbi_fetcher, depends_on='NERPipe')

    def construct_med_graph(self, request_json):
        """ main method """
        outputs = list()
        # get disease and possible filter
        disease, request_kwargs = self._parse_request(request_json)

        pipe_lines = ['StandardPubMedPipe','NERPipe','MedGenPipe']

        # get articles
        pubmed_paper = self.ncbi_fetcher.get_pubmed_paper(
            disease,
            n_articles=request_kwargs['n_articles'] if 'n_articles' in request_kwargs.keys() else None
        )

        # build dataframe for pubmed data
        pubmed_output = self.pubmed_pipe.run(paper=pubmed_paper, search_term=disease, node_label='Paper')
        outputs.append(pubmed_output)
        # extract named entities and entity links to UMLS knowledgebase
        ner_output = self.ner_pipe.run(
            abstracts=pubmed_output.get_table('pubmedPaper'), id_col='pubmedID', abstract_col='abstract'
        )
        outputs.append(ner_output)
        df_entity = ner_output.get_table('Entities')
        if 'MedGenPipe' in pipe_lines:
            df_links = ner_output.get_table('UmlsLinks')
            # fetch data from MedGen
            medgen_output = self.medgen_pipe.run(
                df_entities=df_entity, df_links=df_links, snomed=True, clinical_features=True
            )
            outputs.append(medgen_output)

        return disease, outputs

    def _parse_request(self, request_json: str) -> tuple:
        """
        get info from json
        :param request_json: json
        :return:
        """
        request_data = json.loads(request_json)
        missing_args = [x for x in self.REQUIRED_REQUEST_ARGS if x not in request_data.keys()]
        if missing_args:
            raise RuntimeError(f'Missing required parameters in request: {missing_args}')
        disease = request_data.pop(self.DISEASE)
        # TODO Oweys: get pipelines from json and other keywords
        """
        So koennte ich mir vorstellen, dass ein request JSON von Lorenz Frontend aussehen wird.
        Die Argumente für die Pipes können schon teilweise einfach in deren run() Methode übergeben werden, wie für
        die medGen pipe oder müssen noch implementiert werden.
        
           example_json = {
                'disease': 'phenylketonurie',
                'n_articles': 100, # number of articles to be fetched
                'pipelines': {
                    'pubmed': {
                        'run': True,  # this database is required and must be set
                        'meshTerms': True  # flag if MeSH terms shall be extracted
                        },
                    'ner': {
                        'run': True,
                        'entityLinks': True  # flag if links to UMLS knowledge base shall be extracted --> required for farther pipelines
                        },
                    'medGen': {
                        'run': True,
                        'Snomed': True, # flag if SnomedConcepts are extracted
                        'clinicalFeatures': False # flag for clinical Features
                    },
                    'uniProt': {} # TODO: Sönke muss die noch implementieren
                }
            }
        """
        # the following statements can be deleted
        # parse pipelines
        pipelines = list()
        if request_data.get('mesh_terms'):
            pipelines.append(ExtractionPipe('mesh_terms', get_mash_terms))
        if request_data.get('key_words'):
            pipelines.append(ExtractionPipe('key_words', get_keywords))
        if request_data.get('entities'):
            pipelines.append(ExtractionPipe('entities', self.ner_pipe.run))
        request_data['extraction_pipe'] = pipelines

        return disease, request_data

    @staticmethod
    def _read_config(cfg_path: str) -> dict:
        if not os.path.isfile(cfg_path):
            raise AttributeError('Cannot find file under given config path:', cfg_path)
        if not cfg_path.endswith('.json'):
            raise RuntimeError('Config is expected to be a json file, but the following was given:', cfg_path)
        with open(cfg_path, 'r') as fh:
            cfg = json.load(fh)
        return cfg
