import json
from flask import Flask, request, abort
from flask_cors import CORS, cross_origin
from dotenv import load_dotenv
import os

from pymedgraph.manager import MedGraphManager
from pymedgraph.graph.builder import Neo4jBuilder

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
neo4j = Neo4jBuilder(neo4j_cfg['url'], neo4j_cfg['user'], neo4j_cfg['pw'], logger=logger)


def configure():
    """ Adds security layer to the API
    """
    load_dotenv()
    api_tokens = [os.getenv("key"+str(i)) for i in range(1,5)]
    return api_tokens


@app.route("/", methods=["POST"])
@cross_origin()
def get_json():
    """ Takes and checks an postrequest from the caller and passes it to the backend.
    """
    logger.info('Got request.')
    if request.method == "POST":
        if request.json:
            request_json = request.json
            if ('request_specs' and 'token') in request_json.keys():
                if not request_json['token'] in tokens:
                    logger.error('403: Invalid token.')
                    abort(403, 'Token is invalid.')

                results = send_request(request_json['request_specs'])

                return results
            logger.error('415: JSON data missing request_specs or token field.')
            abort(400, 'JSON data missing request_specs or token field.')
        logger.error('415: No json in request.')
        abort(415)
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
    app.run(port=8050, debug = True)
