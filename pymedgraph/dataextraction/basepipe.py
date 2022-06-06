"""

Gaol: Set a standard for pipelines and pipeline outputs to simplify workflow.

Pipelines:
    - PubMed Paper standard
    - PubMed Abstract NER (with or without entity linkage)
    - MedGen summary --> Gene
    - UniProt

Output: each pipeline should have the same structure of output

Node Class or DataFrame

|source |node label | node attribute 1  | ... |node attribute X  |
|---    |---        |---                | ... |---               |


|source |node label  | $attr$Name  | $attr$UniProtID    |
|GenXY  | ProteinXY  | geneName     | xyIDxy            |


"""
import pandas as pd


class BasePipe(object):

    SOURCE_COL = 'source'
    NODEL_LABEL_COL = 'node_label'
    NODE_ATTR_PREFIX = '$attr$'

    def __init__(self, depends_on=None):
        self.name = 'BasePipe'
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
