import os
import json

from pymedgraph.io.fetch_ncbi import NCBIFetcher
from pymedgraph.dataextraction import (
    MedGraphNER,
    get_mash_terms,
    get_pubmed_id,
    get_keywords,
    get_pubmed_title
)


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
        self.cfg = self._read_config(config_path)
        self.ncbi_fetcher = NCBIFetcher(
            email=self.cfg['NCBI']['email'],
            tool_name=self.cfg['NCBI']['tool_name'],
            max_articles=self.cfg['NCBI']['max_articles']
        )
        self.ner = MedGraphNER()

    def construct_med_graph(self, request_json):
        """ main method """
        paper_dicts = dict()
        # get disease and possible filter
        disease, request_kwargs = self._parse_request(request_json)

        # get articles
        pubmed_paper = self.ncbi_fetcher.get_pubmed_paper(
            disease,
            n_articles=request_kwargs['n_articles'] if 'n_articles' in request_kwargs.keys() else None
        )

        # extract info from response
        for paper in pubmed_paper:
            paper_id = get_pubmed_id(paper)
            paper_title = get_pubmed_title(paper)
            tmp_result_dict = {'title': paper_title}
            # run pipelines
            for pipe in request_kwargs['extraction_pipe']:
                tmp_result_dict[pipe.name] = pipe.run_pipe(paper)
            # store results
            paper_dicts[paper_id] = tmp_result_dict

        return paper_dicts

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
            pipelines.append(ExtractionPipe('entities', self.ner.ner_pipe))
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
