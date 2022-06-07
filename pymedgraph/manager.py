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

        # get disease and possible filter
        disease, request_kwargs = self._parse_request(request_json)

        pipe_lines = ['StandardPubMedPipe','NERPipe','MedGenPipe']

        # get articles
        pubmed_paper = self.ncbi_fetcher.get_pubmed_paper(
            disease,
            n_articles=request_kwargs['n_articles'] if 'n_articles' in request_kwargs.keys() else None
        )

        # build dataframe for pubmed data
        df_pubmed = self.pubmed_pipe.run(paper=pubmed_paper, search_term=disease, node_label='Paper')
        output = [df_pubmed]
        # extract named entities and entity links to UMLS knowledgebase
        ner_output = self.ner_pipe.run(abstracts=df_pubmed, id_col='$attr$pubmedID', abstract_col='$attr$abstract')
        df_entity = ner_output[0]
        output.append(df_entity)
        if 'MedGenPipe' in pipe_lines:
            df_links = ner_output[1]
            output.append(df_links)
            # fetch data from MedGen
            medgen_dfs = self.medgen_pipe.run(
                df_entities=df_entity, df_links=df_links, snomed=True, clinical_features=True
            )
            output += medgen_dfs

        return output

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
