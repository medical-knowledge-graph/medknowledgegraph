import json
from flask import Flask, render_template, request, abort, jsonify
from pymedgraph.manager import MedGraphManager
from pymedgraph.graph.builder import Neo4jBuilder

app = Flask(__name__)

manager = MedGraphManager(config_path='./pymedgraph/localconfig.json')
neo4j_cfg = manager.cfg.get('Neo4j')
neo4j = Neo4jBuilder(neo4j_cfg['url'], neo4j_cfg['user'], neo4j_cfg['pw'])

@app.route("/", methods=["POST"])
def get_json():
    if request.method == "POST":
        if request.json:
            request_json = request.json
            if 'text' in request_json:
                results = send_keyword(request_json['text'])

                return results
            abort(400, 'JSON data missing text field.')
        abort(415)
    abort(405)
    

def send_keyword(keyword):
    req = json.dumps({'disease': keyword})
    # build tables for nodes and node relations
    disease, outputs = manager.construct_med_graph(req)
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
