import pandas as pd
from pymedgraph.dataextraction.basepipe import BasePipe

class UniProtPipe(BasePipe):
    """
    |source |node label  | $attr$Name  | $attr$UniProtID    |
    |GenXY  | ProteinXY  | geneName     | xyIDxy            |
    """
    def __init__(self):
        super().__init__(pipe_name='UniProtPipe')

    def _run_pipe(self, **kwargs) -> pd.DataFrame:
        # TODO: implemente here
        pass
