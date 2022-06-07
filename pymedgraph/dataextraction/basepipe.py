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
        output = self._run_pipe(**kwargs)
        self._check_output(output)
        return output

    def _run_pipe(self, **kwargs) -> pd.DataFrame:
        pass

    def _check_output(self, output: pd.DataFrame or list):
        # check if list
        if isinstance(output, list):
            for df in output:
                self._check_df(df)
        # if not list, output should be a pd.DataFrame
        else:
            self._check_df(output)

    def _check_df(self, df: pd.DataFrame):
        # check if dataframe
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f'Output of pipe \'{self.name}\' must be of type pd.DataFrame.')
        # check data for columns
        columns = df.columns
        if self.SOURCE_COL not in columns:
            raise RuntimeError(
                f'Source column \'{self.SOURCE_COL}\' is required, but not found in output columns: {columns}.'
            )
        if self.NODEL_LABEL_COL not in columns:
            raise RuntimeError(
                'Nodel label column \'{nlc}\' is required, but not found in output columns: {c}.'.format(
                    nlc=self.NODEL_LABEL_COL, c=columns)
            )
        if not [c for c in columns if c.startswith(self.NODE_ATTR_PREFIX)]:
            raise RuntimeError(
                'Output requires at least one Attribute column with prefix \'{n}\' but was not found in: {c}.'.format(
                    n=self.NODE_ATTR_PREFIX, c=columns)
            )

    def _set_column_names(self, columns: list) -> dict:
        return {col: self._attr_column(col) for col in columns}

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
