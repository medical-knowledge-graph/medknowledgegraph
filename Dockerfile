# syntax=docker/dockerfile:1
FROM python:3.7
WORKDIR /app
# isntall requirements
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
# install pymedgraph package
COPY . .
RUN pip install -e .
# downlaod scispacy bc5cdr model
RUN pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.0/en_ner_bc5cdr_md-0.5.0.tar.gz
# install notebook -> no use in production
RUN pip install notebook
