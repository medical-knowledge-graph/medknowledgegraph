import spacy
import scispacy

from pymedgraph.dataextraction.parser import parse_pubmed_article

class MedGraphNER(object):
    """ Class to process abstracts with `scispacy` model """
    def __init__(self,  model_name: str = 'en_ner_bc5cdr_md'):
        self.nlp_name = model_name
        self.nlp = self._load_nlp(model_name)

    def ner_pipe(self, paper: dict) -> list:
        """
        :param paper: dict -
        :return: list - contains tuples (e, l) e-> found entity, l-> entity label
        """
        # get abstract text from paper
        abstract = parse_pubmed_article(paper)
        # process
        doc = self.nlp(abstract, disable=["tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer"])
        entities = [(e.text, e.label_) for e in doc.ents]
        # return unique
        return list(set(entities))

    @staticmethod
    def _load_nlp(model_name):
        nlp = spacy.load(model_name)
        return nlp