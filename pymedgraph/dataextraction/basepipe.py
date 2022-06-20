import pandas as pd
from pymedgraph.dataextraction.parser import get_pubmed_id, get_pubmed_title, get_mash_terms, parse_pubmed_article


class NodeTable(object):
    """
    Class is used to store a node data table as pd.DataFrame with meta data info, such as node label of source etc
    """
    def __init__(self, name, df, source_node, source_node_attr, source_col, node_label, id_attribute, attribute_cols):
        """

        :param name: str - name of node table. Should be used as identifier, if you are creating a table for gene nodes
        you want to name the NodeTable instance `genes`.
        :param df: pd.DataFrame - contains info about the node. See above
        :param source_node: str or list
        :param source_node_attr: str
        :param source_col: str
        :param node_label: str or list
        :param id_attribute: str
        :param attribute_cols: list
        """
        self.name = name
        self.meta = {
            'table_name': name,
            'source_node': source_node,
            'source_node_attr': source_node_attr,
            'source_column': source_col,
            'node_label': node_label,
            'id_attribute': id_attribute,
            'attribute_cols': attribute_cols
        }
        self._check_df(df)
        self.data = df

    @property
    def meta(self):
        return self._meta

    @meta.setter
    def meta(self, m: dict):
        if not isinstance(m, dict):
            raise TypeError('NodeTable.meta must be a dict.')

        req_keys = ['table_name', 'source_node', 'source_node_attr', 'source_column',
                    'node_label', 'id_attribute', 'attribute_cols']
        missing_keys = [k for k in req_keys if k not in m.keys()]
        if missing_keys:
            raise AttributeError(f"NoteTable.meta is missing following keys: {missing_keys}")
        if m['attribute_cols'] and not isinstance(m['attribute_cols'], list):
            raise TypeError('NoteTable.meta["attribute_cols"] must be a list.')
        # keys which require a value
        req_vals = ['table_name', 'node_label', 'id_attribute']
        for k, v in m.items():
            if k in req_vals and v is None:
                raise AttributeError(f'NoteTable.meta["{k}"] is not allowed to be None.')
        if m['source_column'] and (m['source_node'] is None or m['source_node_attr'] is None):
            raise AttributeError(
                f'If NoteTable.meta["source_column"] is set,'
                f'"source_node" and "source_node_attr" is not allowed to be None.'
            )
        self._meta = m

    def _check_df(self, df: pd.DataFrame):
        # check if dataframe
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f'Output of pipe \'{self.name}\' must be of type pd.DataFrame.')
        # check data for columns
        columns = df.columns
        req_cols = [self.meta['source_column'], 'node_label', self.meta['id_attribute']]
        if self.meta['attribute_cols']:
            req_cols += self.meta['attribute_cols']
        for col in req_cols:
            if col not in columns:
                raise RuntimeError(f'Given pd.DataFrame is missing required column \'{col}\'.')
        if isinstance(self.meta['node_label'], list):
            if list(df['node_label'].unique()) != self.meta['node_label']:
                raise RuntimeError(
                    'Found unexpected values in df["node_label"] {unq_vals}. Expected is \'{nl}\''.format(
                        unq_vals=df['node_label'].unique(), nl=self.meta["node_label"]
                    )
                )
        else:
            if df['node_label'].nunique() > 1:
                raise RuntimeError('Found unexpected node labels {unq_vals}. Expects: {nl}'.format(
                    unq_vals=df['node_label'].unique(), nl=self.meta["node_label"]
                ))
            if df['node_label'].unique()[0] != self.meta['node_label']:
                raise RuntimeError('Found NodeLabel \'{fnl}\' is not expected node label: \'{nl}\''.format(
                    fnl=df['node_label'].unique()[0], nl=self.meta['node_label']
                ))


class PipeOutput(object):
    """ Class to store pymedgraph.dataextraction.basepipe.NodeTable outputs """
    def __init__(self, pipe: str):
        self.pipe = pipe
        self.node_tables = list()

    def add(self, node_table: NodeTable):
        if not isinstance(node_table, NodeTable):
            raise TypeError('Can only add objects of type NodeTable.')
        self.node_tables.append(node_table)

    def get_table(self, name: str) -> pd.DataFrame:
        for table in self.node_tables:
            if table.name == name:
                return table.data
        return f'Output for pipe {self.pipe} does not contain a NodeTable with name: \'{name}\'.'


class BasePipe(object):
    """

    Gaol: Set a standard for pipelines and pipeline outputs to simplify workflow.

    Pipelines:
        - PubMed Paper standard
        - PubMed Abstract NER (with or without entity linkage)
        - MedGen summary --> Gene
        - UniProt

    Output: each pipeline should have the same structure of output --> NodeTable

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

    def run(self, **kwargs) -> PipeOutput:
        output = self._run_pipe(**kwargs)
        self._check_output(output)
        return output

    def _run_pipe(self, **kwargs) -> PipeOutput:
        pass

    def _check_output(self, output: pd.DataFrame or list):
        # check if list
        #if isinstance(output, list):
        #    for df in output:
        #        self._check_df(df)
        # if not list, output should be a pd.DataFrame
        #else:
        #    self._check_df(output)
        if not isinstance(output, PipeOutput):
            raise TypeError('Pipe output must be an object of type pymedgraph.dataextraction.basepipe.PipeOutput.')

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

    def _run_pipe(self, search_term: str, node_label: str, paper: list, mesh_terms=False) -> PipeOutput:
        """
        Extract info from PubMed Api Response of found and fetched articles.
        """
        output = PipeOutput(self.name)
        paper_entries = list()

        if mesh_terms:
            for pap in paper:
                paper_entries.append(
                    (get_pubmed_id(pap), get_pubmed_title(pap), parse_pubmed_article(pap), get_mash_terms(pap))
                )
            df = pd.DataFrame(paper_entries, columns=self._attribute_columns + ['MeSH'])
        else:
            for pap in paper:
                paper_entries.append(
                    (get_pubmed_id(pap), get_pubmed_title(pap), parse_pubmed_article(pap))
                )
            df = pd.DataFrame(paper_entries, columns=self._attribute_columns)

        # add required data information
        df[self.SOURCE_COL] = search_term
        df[self.NODEL_LABEL_COL] = node_label

        # init NodeTable object and add to Output object
        output.add(NodeTable(
            name='pubmedPaper',
            df=df,
            source_node='SearchTerm',
            source_node_attr='label',
            source_col=self.SOURCE_COL,
            node_label=node_label,
            id_attribute='pubmedID',
            attribute_cols=self._attribute_columns
        ))
        return output
