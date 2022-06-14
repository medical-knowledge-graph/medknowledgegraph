import json
from flask import Flask, render_template, request, abort, jsonify
from pymedgraph.manager import MedGraphManager

app = Flask(__name__)


@app.route("/", methods=["POST"])
def get_json():
    if request.method == "POST":
        if request.json:
            request_json = request.json
            if 'text' in request_json:
                results = send_keyword(request_json['text'])

                return res
            abort(400, 'JSON data missing text field.')
        abort(415)
    abort(405)
    

def send_keyword(keyword):
    req = json.dumps({'disease': keyword})
    manager = MedGraphManager(config_path='./pymedgraph/localconfig.json')
    res = manager.construct_med_graph(request_json)
    
    return res

    
if __name__ == "__main__":
    app.run(port=9000, debug = True)