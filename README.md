# biomedicalknowledgegraph

# pymedgraph
Python package to fetch data from ncbi sources, retrieve named entities (NER) from abstracts and build graph structure based on
received data.
## package structure
- pymedgraph.io:
  - get data
- pymedgraph.dataextraction:
  - model and methods to get NER from abstracts
- pymedgraph.graph:
  - build graph relations

# install

# run notebook on docker
Make sure you have docker installed!
move to package dir
````shell
cd /your/path/to/medknowledgegraph
````
build docker image
````shell
docker build --tag python-med .
````
build container
```shell
docker run -it -p 8888:8888 python-med:latest
```
run notebook within container cli
````shell
jupyter notebook --ip 0.0.0.0 --no-browser --allow-root
````
copy output link from container cli output into browser.
links looks similar to http://127.0.0.1:8888/?token=0e9dff5b82c10cf569fb6a33195e04c61923dabbf6234c14