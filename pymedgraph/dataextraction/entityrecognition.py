import spacy
import scispacy
from scispacy.linking import EntityLinker  # do NOT remove this import
from scispacy.abbreviation import AbbreviationDetector  # do NOT remove this import

import pandas as pd

from pymedgraph.dataextraction.basepipe import BasePipe, PipeOutput, NodeTable


class NERPipe(BasePipe):
    """

    Pipeline to extract named entities from abstracts with scispacy model.

    If entity linker for UMLS is set, found entities are linked to knowledge base

    """
    def __init__(self, nlp_model: str, entity_linker: str = None, depends_on=None):
        super().__init__('NERPipe', depends_on)

        # load scispacy model
        self.nlp =  spacy.load(nlp_model)
        if entity_linker:
            self.nlp.add_pipe("abbreviation_detector")
            self.nlp.add_pipe("scispacy_linker", config={"resolve_abbreviations": True, "linker_name": entity_linker})
            self.linker = self.nlp.get_pipe("scispacy_linker")
        else:
            self.linker = None

        self.columns = self._set_column_names(['text', 'CUI', 'Definition', 'name'])

    def _run_pipe(self, abstracts: pd.DataFrame, id_col: str, abstract_col: str) -> PipeOutput:
        """
        Extract NamedEntities and Linkage to UMLS Knowledgebase
        """
        output = PipeOutput(self.name)

        named_entities = list() # contains tuples (i, t, l) i=paper id, t=entity text, l=entity label
        entity_links = set()  # contains tuples (t, c, s) t=entity text, c=CUI of entity link, s=score of entity link

        # go through abstracts
        for paper_id, doc in zip(
                abstracts[id_col],
                self.nlp.pipe(
                    abstracts[abstract_col].values,
                    disable=["tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer"]
                )
        ):
            paper_ents = set()
            for entity in doc.ents:
                # add named entities
                paper_ents.add((paper_id, entity.text, entity.label_))
                # loop over entity links to knowledge base
                if self.linker and entity._.kb_ents:
                    for kb_ent in entity._.kb_ents:
                        entity_links.add((entity.text, kb_ent[0], kb_ent[1]))

            # append to list
            named_entities += list(paper_ents)

        # build DataFrames
        df_entities = pd.DataFrame(
            named_entities,
            columns=[self.SOURCE_COL, 'text', self.NODEL_LABEL_COL]
        )

        # text lower and remove possible duplicates but with subset for `pubmedID` and `text` cols
        df_entities['text'] = df_entities['text'].str.lower()
        df_entities.drop_duplicates(subset=['text', self.SOURCE_COL], inplace=True)

        output.add(NodeTable(
            name='Entities',
            df=df_entities,
            source_node='Paper',
            source_node_attr='pubmedID',
            source_col=self.SOURCE_COL,
            node_label=list(df_entities[self.NODEL_LABEL_COL].unique()),
            id_attribute='text',
            attribute_cols=''
        ))

        if entity_links:
            df_entity_links = self._build_entity_links_df(entity_links)
            output.add(NodeTable(
                name='UmlsLinks',
                df=df_entity_links,
                source_node=list(df_entities[self.NODEL_LABEL_COL].unique()),
                source_node_attr='text',
                source_col=self.SOURCE_COL,
                node_label='UMLS',
                id_attribute='CUI',
                attribute_cols=['name', 'Definition']
            ))

        return output

    def _build_entity_links_df(self, entity_links: set) -> pd.DataFrame:
        # build df
        df = pd.DataFrame(entity_links, columns=[self.SOURCE_COL, 'CUI', 'kb_score'])
        # add umls concept name
        df['name'] = df['CUI'].apply(lambda x: self.linker.kb.cui_to_entity[x].canonical_name)
        # .. and definition
        df['Definition'] = df['CUI'].apply(lambda x: self.linker.kb.cui_to_entity[x].definition)
        df[self.NODEL_LABEL_COL] = 'UMLS'
        df[self.SOURCE_COL] = df[self.SOURCE_COL].str.lower()
        df.drop_duplicates(subset=['CUI', self.SOURCE_COL], inplace=True)
        return df
