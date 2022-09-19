import os
import json

from pymedgraph.input.fetch_ncbi import NCBIFetcher
from pymedgraph.dataextraction import StandardPubMedPipe, NERPipe, MedGenPipe, UniProtPipe
from pymedgraph.utils import store_medgen_genes_set


class MedGraphManager(object):
    """
    Class to manage api requests and the data fetching and processing to build data tables for a neo4j Knowledge Graph.
    The data processing are organised in pipelines, which are initialized by the initiation of this class.
    The api request is handled by the method `MedGraphManager.construct_med_graph()`.
    """

    DISEASE = 'disease'
    REQUIRED_REQUEST_ARGS = [DISEASE, 'pipelines']
    PIPE_HIERARCHY = ['pubmed', 'ner', 'medGen', 'uniProt']

    def __init__(self, config_path: str = 'localconfig.json', logger=None):
        """
        Inits MedGraphManager based and pipelines based on the passed config.
        It is necessary to init the pipelines now, because of the great loading time of the NERPipe, meaning for one
        the scispacy model but more importantly the `EntityLinker` which requires several minutes.

        :param config_path: Takes credentials for NCBI databases and MedGenPipe specs
        :param logger: logger
        """
        # logger
        self.logger = logger

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
        if self.cfg['pipes'].get('medgen'):
            medgen_cfg = self.cfg['pipes']['medgen']
            if 'medgen_list_path' in medgen_cfg.keys():
                medgen_path = medgen_cfg['medgen_list_path']
                if not os.path.isfile(medgen_path):
                    store_medgen_genes_set(medgen_path, None)
            self.medgen_pipe = MedGenPipe(self.ncbi_fetcher, depends_on='NERPipe', **medgen_cfg)
        else:
            self.medgen_pipe = MedGenPipe(self.ncbi_fetcher, depends_on='NERPipe')
        self.uniprot_pipe = UniProtPipe(depends_on='MedGenPipe')

    def construct_med_graph(self, request_json):
        """
        Main method of the class, which is called by the api. The calls every pipeline accordingly to the received
        request specifications and collects the output.
        The processing is as follows:
        1. fetch data from Pubmed
        2. StandardPubMedPipe: get data from basic info from response
        3. NERPipe: extract CHEMICAl & DISEASE entities from abstracts and searches for UMLS concepts of DISEASE entities
        4. MedGenPipe: collects promising UMLS Concept ID`s (CUI) and makes MedGen request plus extracts data
        5. UniProtPipe: makes request based on genes from MedGenPipe and extracts Proteins + GenOntologies

        The output of each pipe is a `pymedgraph.dataextraction.basepipe.PipeOutput` object containing n
        `pymedgrapg.dataextraction.basepipe.NodeTable`. These objects are used for the neo4j upload.

        :param request_json:
            Json request passed from frontend.

        :return: disease, outputs, delete_graph_flag:
            Returns collected diseases and other outputs created by the pipelines.
        """
        outputs = list()
        # get disease and possible filter
        disease, pipe_cfg = self._parse_request(request_json)

        if self.logger:
            self.logger.info(f'*** START processing pipelines for \'{disease}\' ****')
            self.logger.info('With pipe config: {cfg}'.format(cfg=pipe_cfg))

        # get articles
        pubmed_paper = self.ncbi_fetcher.get_pubmed_paper(
            disease,
            n_articles=pipe_cfg['n_articles'] if 'n_articles' in pipe_cfg.keys() else None
        )

        if self.logger:
            self.logger.info('Successfully fetched pubmed articles.')

        # build dataframe for pubmed data
        pubmed_output = self.pubmed_pipe.run(
            paper=pubmed_paper,
            search_term=disease,
            node_label='Paper',
            mesh_terms=pipe_cfg['pipelines']['pubmed']['meshTerms']
        )
        outputs.append(pubmed_output)
        if self.logger:
            self.logger.info('Successfully extracted paper from response.')
        # extract named entities and entity links to UMLS knowledgebase
        ner_output = self.ner_pipe.run(
            abstracts=pubmed_output.get_table('pubmedPaper'), id_col='pubmedID', abstract_col='abstract'
        )
        if self.logger:
            self.logger.info('Successfully extracted entities from abstracts.')
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
            if self.logger:
                self.logger.info('Successfully extracted medGen data.')
        if 'uniProt' in pipe_cfg['pipelines'].keys():
            genes = medgen_output.get_table('Genes')['gene'].tolist()
            if genes:
                uniprot_output = self.uniprot_pipe.run(genes=genes)
                outputs.append(uniprot_output)
                if self.logger:
                    self.logger.info('Successfully extracted gene data from UniProt.')
        return disease, outputs, pipe_cfg['delete_existing_graph']

    def _parse_request(self, request_json: str) -> tuple:
        """
        Method to parse request json to search term and config for the following pipelines.
        Config is then checked for logical errors

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

        :param request_json: Request in a Json-File
        :return: Returns diseases and requested data
        """
        pipe_run_cfg = dict()  # dictionary of pipe info
        # check if no json but dict was passed
        if isinstance(request_json, dict):
            request_data = request_json.copy()
        else:
            request_data = json.loads(request_json)
        missing_args = [x for x in self.REQUIRED_REQUEST_ARGS if x not in request_data.keys()]
        if missing_args:
            if self.logger:
                self.logger.error(f'Missing required parameters in request: {missing_args}')
            raise RuntimeError(f'Missing required parameters in request: {missing_args}')
        # extract info from request dict
        disease = request_data.pop(self.DISEASE)
        pipe_run_cfg['n_articles'] = request_data['n_articles'] if 'n_articles' in request_data.keys() else self.cfg['NCBI']['max_articles']
        pipe_run_cfg['delete_existing_graph'] = request_data['delete_graph'] if 'delete_graph' in request_data.keys() else False
        pipes = dict()
        for pipe, v in request_data['pipelines'].items():
            if v['run']:
                # special MedGen case
                if pipe == 'medGen':
                    for k in ['Snomed', 'clinicalFeatures']:
                        if k not in v:
                            v[k] = False
                if pipe == 'pubmed':
                    if 'meshTerms' not in v:
                        v['meshTerms'] = False
                # add pipe value to dict
                pipes[pipe] = v
        pipe_run_cfg['pipelines'] = pipes
        self._check_pipeline(list(pipes.keys()))

        return disease.lower(), pipe_run_cfg

    def _read_config(self, cfg_path: str or dict) -> dict:
        """ Reads config file.

        :param cfg_path:
            Path to config file or config dict

        :return cfg:
            Returns the config dictionary.
        """
        if isinstance(cfg_path, dict):
            return cfg_path
        else:
            if not os.path.isfile(cfg_path):
                if self.logger:
                    self.logger.error(f'RuntimeError: Cannot find file under given config path: {cfg_path}')
                raise RuntimeError('Cannot find file under given config path:', cfg_path)
            if not cfg_path.endswith('.json'):
                if self.logger:
                    self.logger.error(
                        f'RuntimeError: Config is expected to be a json file, but the following was given: {cfg_path}'
                    )
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
                    if self.logger:
                        self.logger.error( 'Pipe \'{p}\' is set in request but required predecessor pipe \'{pp}\' is missing.'.format(
                        p=p, pp=rev_hierarchy[i+1]))
                    raise RuntimeError(
                        'Pipe \'{p}\' is set in request but required predecessor pipe \'{pp}\' is missing.'.format(
                        p=p, pp=rev_hierarchy[i+1]
                    ))
