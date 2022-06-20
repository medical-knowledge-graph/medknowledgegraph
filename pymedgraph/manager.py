import os
import json

from pymedgraph.input.fetch_ncbi import NCBIFetcher
from pymedgraph.dataextraction import (
    get_mash_terms,
    get_pubmed_id,
    get_keywords,
    get_pubmed_title
)
from pymedgraph.dataextraction import StandardPubMedPipe, NERPipe, MedGenPipe



class MedGraphManager(object):
    """ Class to manage requests and graph build"""

    DISEASE = 'disease'
    REQUIRED_REQUEST_ARGS = [DISEASE, 'pipelines']
    PIPE_HIERARCHY = ['pubmed', 'ner', 'medGen', 'uniProt']

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
        disease, pipe_cfg = self._parse_request(request_json)

        # get articles
        pubmed_paper = self.ncbi_fetcher.get_pubmed_paper(
            disease,
            n_articles=pipe_cfg['n_articles'] if 'n_articles' in pipe_cfg.keys() else None
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
        if 'medGen' in pipe_cfg['pipelines'].keys():
            df_links = ner_output.get_table('UmlsLinks')
            # fetch data from MedGen
            medgen_output = self.medgen_pipe.run(
                df_entities=df_entity,
                df_links=df_links,
                snomed=pipe_cfg['pipelines']['medGen']['Snomed'],
                clinical_features=pipe_cfg['pipelines']['medGen']['clinicalFeatures']
            )
            outputs.append(medgen_output)

        return disease, outputs

    def _parse_request(self, request_json: str) -> tuple:
        """
        get info from json

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
                    'uniProt': {'run': False}
                }
            }

        :param request_json: json
        :return:
        """
        pipe_run_cfg = dict()  # dictionary of pipe info
        request_data = json.loads(request_json)
        missing_args = [x for x in self.REQUIRED_REQUEST_ARGS if x not in request_data.keys()]
        if missing_args:
            raise RuntimeError(f'Missing required parameters in request: {missing_args}')
        disease = request_data.pop(self.DISEASE)
        pipe_run_cfg['n_articles'] = request_data['n_articles'] if 'n_articles' in request_data.keys() else self.cfg['NCBI']['max_articles']
        pipes = dict()
        for pipe, v in request_data['pipelines'].items():
            if v['run']:
                # special MedGen case
                if pipe == 'medGen':
                    for k in ['Snomed', 'clinicalFeatures']:
                        if k not in v:
                            v[k] = False
                # add pipe value to dict
                pipes[pipe] = v
        pipe_run_cfg['pipelines'] = pipes
        self._check_pipeline(list(pipes.keys()))

        return disease, pipe_run_cfg

    @staticmethod
    def _read_config(cfg_path: str) -> dict:
        if not os.path.isfile(cfg_path):
            raise AttributeError('Cannot find file under given config path:', cfg_path)
        if not cfg_path.endswith('.json'):
            raise RuntimeError('Config is expected to be a json file, but the following was given:', cfg_path)
        with open(cfg_path, 'r') as fh:
            cfg = json.load(fh)
        return cfg


    def _check_pipeline(self, pipes: list):
        """
        Method checks if pipelines set to True are not missing any predecessor. Necessary because the pipelines have
        a specific hierarchy (see `MedGraphManager.PIPE_HIERARCHY`), meaning the `ner` pipe requires the  output of
        the `pubmed` pipe and so on.

        If any predecessor is missing the method raises a RuntimeError

        :param pipes: list - pipe names of request_json['pipelines'] if pipe['run'] == True
        """
        rev_hierarchy = self.PIPE_HIERARCHY[::-1]   # reverse list
        for i, p in enumerate(rev_hierarchy):
            if p in pipes and i+1 < len(rev_hierarchy):  # check if pipe is set and if not the last pipe
                if rev_hierarchy[i+1] not in pipes: # this is the predecessor check
                    raise RuntimeError(
                        'Pipe \'{p}\' is set in request but required predecessor pipe \'{pp}\' is missing.'.format(
                        p=p, pp=rev_hierarchy[i+1]
                    ))
