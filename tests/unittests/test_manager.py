import json

import pytest
import mock
import os

from pymedgraph.manager import MedGraphManager

def test_construct_medgraph(request_json):
    #TODO
    pass

def test_parse_request(request_json):
    # testing the request parsing
    req_dict = json.loads(request_json)

    with mock.patch('pymedgraph.manager.NERPipe') as MockNER:  # mock NER pipe, because it takes minutes to load
        MockNER.return_value = None
        manager = MedGraphManager('../../pymedgraph/localconfig.json')

        # 1. correct request as json
        disease, pipe_cfg = manager._parse_request(request_json)

        assert disease == req_dict['disease']
        assert len(pipe_cfg['pipelines'].keys()) == 3
        assert 'uniProt' not in pipe_cfg['pipelines']

        # 2. check if dict can be parsed too
        disease, pipe_cfg = manager._parse_request(req_dict)

        assert disease == req_dict['disease']

        # 3. missing vals
        err_req = req_dict.copy()
        with pytest.raises(RuntimeError, match=r'Missing *.'):
            err_req.pop('disease')
            # missing disease key, val
            manager._parse_request(err_req)
        with pytest.raises(RuntimeError, match=r'Missing *.'):
            # missing pipelines key, val
            manager._parse_request(req_dict.pop('pipelines'))


def test_check_pipelines(request_json):

    pipes = ['pubmed', 'ner', 'medGen', 'uniProt']

    with mock.patch('pymedgraph.manager.NERPipe') as MockNER:
        manager = MedGraphManager('../../pymedgraph/localconfig.json')

        # 1. correct order
        manager._check_pipeline(pipes)

        # 2. incorrect order
        with pytest.raises(RuntimeError, match=r'Pipe \'medGen\' is set in request but required predecessor pipe \'ner\' is missing.'):
            manager._check_pipeline(['pubmed', 'medGen'])
        with pytest.raises(RuntimeError, match=r'Pipe \'ner\' is set in request but required predecessor pipe \'pubmed\' is missing.'):
            manager._check_pipeline(['ner', 'medGen'])

def test_init(tmp_path):
    # test for incorrect config file
    with pytest.raises(RuntimeError, match=r'Cannot *.'):
        MedGraphManager('no_file.json')
