import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
from pymedgraph.dataextraction.basepipe import NodeTable, PipeOutput


def test_nodetable_check(nodetable_df):
    meta = {
        'name': 'Gene',
        'source_node': 'UMLS',
        'source_node_attr': 'CUI',
        'source_col': 'source',
        'node_label': 'Gene',
        'id_attribute': 'gene',
        'attribute_cols': []
    }
    dummy_df = nodetable_df.copy()

    # 1. test missing required column
    with pytest.raises(RuntimeError, match=r'Given pd.DataFrame is missing required column *.'):
        NodeTable(df=dummy_df.drop('source', axis=1).copy(), **meta)

    # 2. test false meta
    with pytest.raises(RuntimeError, match=r'Found unexpected node labels *.'):
        dummy_df['node_label'] = ['Gene', 'FalseLabel']
        NodeTable(df=dummy_df, **meta)

    with pytest.raises(RuntimeError, match=r'Found NodeLabel *.'):
        dummy_df['node_label'] = ['FalseLabel', 'FalseLabel']
        NodeTable(df=dummy_df, **meta)

    # 2. test not allowed column names
    meta['source_col'] = 'source-column'
    dummy_df = nodetable_df.copy()
    dummy_df['source-column'] = nodetable_df['source'].values
    with pytest.raises(ValueError, match=r'Column name \'source-column\' in table *.'):
        NodeTable(df=dummy_df.drop('source', axis=1), **meta)


def test_pipe_output_add(nodetable_df):
    pipe_output = PipeOutput('DummyPipe')

    assert len(pipe_output.node_tables) == 0

    # 1. add correct node table
    pipe_output.add(NodeTable(
        name='Gene',
        df=nodetable_df,
        source_node='UMLS',
        source_node_attr='CUI',
        source_col='source',
        node_label='Gene',
        id_attribute='gene',
        attribute_cols=[]
    ))

    assert len(pipe_output.node_tables) == 1
    assert pipe_output.node_tables[0].meta == {
        'table_name': 'Gene',
        'source_node': 'UMLS',
        'source_node_attr': 'CUI',
        'source_column': 'source',
        'node_label': 'Gene',
        'id_attribute': 'gene',
        'attribute_cols': []
    }
    assert_frame_equal(pipe_output.node_tables[0].data, nodetable_df)

    # 2. test node table
    table_df = pipe_output.get_table('Gene')
    assert_frame_equal(table_df, nodetable_df)

    # 3. try to add only df
    with pytest.raises(TypeError, match=r'Can only *.'):
        pipe_output.add(pd.DataFrame({'colA': [1,2,3,4]}))


def test_nodetable_init(nodetable_df):

    # 1. simple node table
    node_table = NodeTable(
        name='Gene',
        df=nodetable_df,
        source_node='UMLS',
        source_node_attr='CUI',
        source_col='source',
        node_label='Gene',
        id_attribute='gene',
        attribute_cols=[]
    )

    assert node_table.meta == {
        'table_name': 'Gene',
        'source_node': 'UMLS',
        'source_node_attr': 'CUI',
        'source_column': 'source',
        'node_label': 'Gene',
        'id_attribute': 'gene',
        'attribute_cols': []
    }

    with pytest.raises(AttributeError):
        node_table.meta = {
            'table_name': None,
            'source_node': 'UMLS',
            'source_node_attr': 'CUI',
            'source_column': 'source',
            'node_label': 'Gene',
            'id_attribute': 'gene',
            'attribute_cols': []
        }

    with pytest.raises(AttributeError, match=r'NoteTable.meta is missing following keys: *.'):
        node_table.meta = {
            'source_node': 'UMLS',
            'source_node_attr': 'CUI',
            'source_column': 'source',
            'node_label': 'Gene',
            'id_attribute': 'gene',
            'attribute_cols': []
        }


def test_pipe_output_init():
    pipe_output = PipeOutput('DummyPipe')

    assert pipe_output.pipe == 'DummyPipe'
    assert pipe_output.node_tables == []

    # init without name
    with pytest.raises(TypeError):
        PipeOutput()