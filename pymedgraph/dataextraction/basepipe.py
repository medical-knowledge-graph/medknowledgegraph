import pandas as pd
from pymedgraph.dataextraction.parser import get_pubmed_id, get_pubmed_title, get_mash_terms, parse_pubmed_article


class BasePipe(object):
    """

    Gaol: Set a standard for pipelines and pipeline outputs to simplify workflow.

    Pipelines:
        - PubMed Paper standard
        - PubMed Abstract NER (with or without entity linkage)
        - MedGen summary --> Gene
        - UniProt

    Output: each pipeline should have the same structure of output

    DataFrame

    |source |node label | node attribute 1  | ... |node attribute X  |
    |---    |---        |---                | ... |---               |
    """
    SOURCE_COL = 'source'
    NODEL_LABEL_COL = 'node_label'
    NODE_ATTR_PREFIX = '$attr$'

    def __init__(self, pipe_name='BasePipe', depends_on=None):
        self.name = pipe_name
        self.depends_on = depends_on

    def run(self, **kwargs) -> pd.DataFrame:
        df = self._run_pipe(**kwargs)
        self._check_output(df)
        return df

    def _run_pipe(self, **kwargs) -> pd.DataFrame:
        pass

    def _check_output(self, df: pd.DataFrame):
        # check if dataframe
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f'Output of pipe \'{self.name}\' must be of type pd.DataFrame.')
        # check data for columns
        for col in df.columns:
            if col != self.SOURCE_COL and col != self.NODEL_LABEL_COL and not col.startswith(self.NODE_ATTR_PREFIX):
                raise RuntimeError(f'Found unexpected column name \'{col}\' in output of pipe \'{self.name}\'.')

    def _attr_column(self, column):
        return self.NODE_ATTR_PREFIX + column


class StandardPubMedPipe(BasePipe):
    """ Pipeline to get data from PubMed Articles """
    def __init__(self):
        super().__init__(pipe_name='StandardPubMedPipe')
        self._attribute_columns = ['pubmedID', 'title', 'abstract']

    def _run_pipe(self, search_term: str, node_label: str, paper: list, mesh_terms=False) -> pd.DataFrame:
        """
        Extract info from PubMed Api Response of found and fetched articles.
        """
        paper_entries = list()

        if mesh_terms:
            for pap in paper:
                paper_entries.append(
                    (get_pubmed_id(pap), get_pubmed_title(pap), parse_pubmed_article(pap), get_mash_terms(pap))
                )
            df = pd.DataFrame(paper_entries, columns=[self._attr_column(c) for c in self._attribute_columns + ['MeSH']])
        else:
            for pap in paper:
                paper_entries.append(
                    (get_pubmed_id(pap), get_pubmed_title(pap), parse_pubmed_article(pap))
                )
            df = pd.DataFrame(paper_entries, columns=[self._attr_column(c) for c in self._attribute_columns])

        # add required data information
        df[self.SOURCE_COL] = search_term
        df[self.NODEL_LABEL_COL] = node_label

        return df
