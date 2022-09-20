import json
from flask import Flask, request, abort
from flask_cors import CORS, cross_origin
from dotenv import load_dotenv
import os

from pymedgraph.manager import MedGraphManager
from pymedgraph import Neo4jConnector

import logging
from logging.handlers import TimedRotatingFileHandler

# init logging
LOG_DIR = 'logs'

logger = logging.getLogger()

if not os.path.isdir(LOG_DIR):
    raise ValueError('Log dir: \'{lg}\' does not exist, exit.'.format(lg=LOG_DIR))
handler = TimedRotatingFileHandler(
    filename=os.path.join(LOG_DIR, 'pymedgraphAPI.log'),
    backupCount=3
)
logger.addHandler(handler)
handler.setFormatter(
    logging.Formatter('%(asctime)s %(levelname)s %(name)s %(funcName)s -- %(message)s')
)
logger.setLevel(logging.INFO)

# init api
app = Flask(__name__)
# init classes
manager = MedGraphManager(config_path='localconfig.json', logger=logger)
neo4j_cfg = manager.cfg.get('Neo4j')
neo4j = Neo4jConnector(neo4j_cfg['url'], neo4j_cfg['user'], neo4j_cfg['pw'], logger=logger)


def configure():
    """ Adds security layer to the API
    """
    load_dotenv()
    api_tokens = [os.getenv("key"+str(i)) for i in range(1,5)]
    return api_tokens


@app.route("/buildGraph", methods=["POST"])
@cross_origin()
def build_graph():
    """
    Takes and checks an postrequest from the caller and passes it to the backend.
    """
    logger.info('Got \'buildGraph\' request.')
    # check request
    req_json = _get_request_json(request, ['request_specs','token'])
    req_specs = req_json['request_specs']
    # start backend processing
    logger.info(f'*** STARTING to process request \'{req_specs}\'. ***')
    # build tables for nodes and node relations
    disease, outputs, delete_graph = manager.construct_med_graph(req_specs)
    if outputs:
        try:
            # upload tables to neo4j database
            neo4j.build_biomed_graph(disease, outputs, delete_graph)
            logger.info(f'Successfully uploaded graph with search term \'{disease}\' to neo4j.')
            msg = 'success'
        except RuntimeError:
            logger.error('RuntimeError, while upload of graph to neo4j.')
            msg = 'fail'
    else:
        logger.error('Received empty list of outputs from pymedgraph.manager.construct_med_graph().')
        msg = 'fail'
    return msg


@app.route("/searchTerms",  methods=["GET"])
@cross_origin()
def get_searchterms():
    """
    Api returns list of SearchTerm Nodes in Graph
    """
    logger.info('Got \'searchTerms\' request.')
    # check if request is OK
    _check_get_args(request.args, ['token'])
    search_terms = neo4j.get_search_terms()
    return json.dumps({'searchTerms': search_terms})


@app.route("/intersection", methods=["GET"])
@cross_origin()
def get_intersections():
    """
    API returns json with intersection count of nodes for given parameters (search terms and KG level)
    """
    logger.info('Got \'intersection\' request.')
    _check_get_args(request.args, ['searchTerms', 'level', 'token'])
    result = neo4j.get_intersection(request.args.get('searchTerms'), request.args.get('level'))
    return json.dumps(result)

def _check_get_args(request_args, required_args: list = None):
    """
    Method checks get request for parameters
    :param request_args: werkzeug.datastructures.MultiDict
    :param required_args: list
    """
    if required_args is None:
        required_args = ['token']
    if 'token' not in required_args:
        required_args.append('token')
    missing_args = [a for a in required_args if a not in request_args]
    if len(missing_args) > 0:
        logger.error('400: Missing parameter:', missing_args)
        abort(400, 'Missing parameter:', missing_args)
    token = request.args.get('token')
    if not token in tokens:
        logger.error('403: Invalid token.')
        abort(403, 'Token is invalid.')


def _get_request_json(sent_request: request, required_keys: list = None):
    # build list of required keys
    if required_keys is None:
        required_keys = ['token']
    elif 'token' not in required_keys:
        required_keys.append('token')

    # check request
    if sent_request.method == "POST":
        if sent_request.json:
            request_json = sent_request.json
            missing_keys = [k for k in required_keys if k not in request_json.keys()]
            if missing_keys:
                logger.error('415: JSON data is missing:', missing_keys)
                abort(400, 'JSON data is missing:', missing_keys)
            if not request_json['token'] in tokens:
                logger.error('403: Invalid token.')
                abort(403, 'Token is invalid.')
            return request_json
        logger.error('415: No json in request.')
        abort(415)
    else:
        logger.error('405: Not a POST request.')
        abort(405)


def send_request(req_specs):
    """ Creates a MedGraph based on the input of the user.

    :param req_specs:
        Userinput passed via Postrequest.

    :return msg:
        A message about failure or success of the building MedGraph.
    """
    logger.info(f'*** STARTING to process request \'{req_specs}\'. ***')
    # build tables for nodes and node relations
    disease, outputs, delete_graph = manager.construct_med_graph(req_specs)
    if outputs:
        try:
            # upload tables to neo4j database
            neo4j.build_biomed_graph(disease, outputs, delete_graph)
            logger.info(f'Successfully uploaded graph with search term \'{disease}\' to neo4j.')
            msg =  'success'
        except RuntimeError:
            logger.error('RuntimeError, while upload of graph to neo4j.')
            msg = 'fail'
    else:
        logger.error('Received empty list of outputs from manager.construct_med_graph().')
        msg = 'fail'
    return msg


if __name__ == "__main__":
    tokens = configure()
    app.run(port=8050, debug=False)
