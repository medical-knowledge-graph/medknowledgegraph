import json
from flask import Flask, request, abort

from pymedgraph.manager import MedGraphManager
from pymedgraph.graph.builder import Neo4jBuilder

app = Flask(__name__)

manager = MedGraphManager(config_path='./pymedgraph/localconfig.json')
neo4j_cfg = manager.cfg.get('Neo4j')
neo4j = Neo4jBuilder(neo4j_cfg['url'], neo4j_cfg['user'], neo4j_cfg['pw'])


@app.route("/", methods=["POST"])
def get_json():
    """ Takes and checks an postrequest from the caller and passes it to the backend.

    """
    if request.method == "POST":
        if request.json:
            request_json = request.json
            if 'request_specs' in request_json:
                results = send_request(request_json['request_specs'])

                return results
            abort(400, 'JSON data missing request_specs field.')
        abort(415)
    abort(405)
    

def send_request(req_specs):
     """ Creates a MedGraph based on the input of the user.

    :param req_specs:
        Userinput passed via Postrequest.

    :return msg:
        A message about failure or success of the building MedGraph.
    """
    # build tables for nodes and node relations
    disease, outputs = manager.construct_med_graph(req_specs)
    if outputs:
        try:
            # upload tables to neo4j database
            neo4j.build_biomed_graph(disease, outputs)
            msg =  'success'
        except RuntimeError:
            msg = 'fail'
    else:
        msg = 'fail'
    return msg


if __name__ == "__main__":
    app.run(port=9000, debug = True)
