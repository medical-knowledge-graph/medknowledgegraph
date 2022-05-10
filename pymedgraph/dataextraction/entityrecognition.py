import spacy
import scispacy
from scispacy.linking import EntityLinker

from pymedgraph.dataextraction.parser import parse_pubmed_article

class MedGraphNER(object):
    """ Class to process abstracts with `scispacy` model """
    def __init__(self,  model_name: str = 'en_ner_bc5cdr_md', linker_name: str = 'go'):
        self.nlp_name = model_name
        self.nlp = self._load_nlp(model_name, linker_name)
        self.linker = self.nlp.get_pipe('scispacy_linker')

    def ner_pipe(self, paper: dict, entity_links: set) -> tuple:
        """
        :param paper: dict -
        :param entity_links: set -
        :return: list - contains tuples (e, l) e-> found entity, l-> entity label
        """
        entities = list()
        # get abstract text from paper
        abstract = parse_pubmed_article(paper)
        # process
        doc = self.nlp(abstract, disable=["tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer"])
        # entities = [(e.text, e.label_) for e in doc.ents]
        for entity in doc.ents:
            entities.append((entity.text, entity.label_))
            # append links
            for kb_ent in entity._.kb_ents:
                # append to set
                entity_links.add((
                    entity.text,
                    kb_ent[0],
                    self.linker.kb.cui_to_entity[kb_ent[0]].canonical_name,
                    self.linker.kb.cui_to_entity[kb_ent[0]].definition
                ))
        # return unique
        return list(set(entities)), entity_links

    @staticmethod
    def _load_nlp(model_name: str, linker_name: str) -> spacy.language.Language:
        nlp = spacy.load(model_name)
        nlp.add_pipe("scispacy_linker", config={"resolve_abbreviations": True, "linker_name": linker_name})
        return nlp