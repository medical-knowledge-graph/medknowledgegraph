import pandas as pd
from pymedgraph.dataextraction.basepipe import BasePipe, PipeOutput, NodeTable
from pymedgraph.dataextraction.uniprotcolumns import UNIPROT_COLS
from pymedgraph.input.uniprot import get_uniprot_entry, get_uniprot_results


class UniProtPipe(BasePipe):
    """
    This class extracts data from the bioinformatic protein database `UniProt` based on passed genes.
    The pipe will always return proteins and if the correct flag is set, we extract gene ontologies (GO), too.

    ** IMPORTANT **
    Make sure that column names fit the `Returned Fields` of uniprot: https://www.uniprot.org/help/return_fields
    If you want to make changes go to pymedgraph.dataextraction.uniprotcolumns and adapt UNIPROT_COLS
    **
    """

    UNIPROT_URL = 'https://www.uniprot.org/uniprotkb/'
    GO_TYPES = ['molecular function', 'biological process', 'cellular component']

    def __init__(self, depends_on=None):
        super().__init__(pipe_name='UniProtPipe', depends_on=depends_on)

    def _run_pipe(self, genes: list, go=True, refseq=False) -> PipeOutput:
        """
        This method makes a request to the UniProt database, based on the passed list of genes. From the response
        we can extract proteins and if the flag is set, gene ontologies (GO) as well.
        """
        output = PipeOutput(self.name)
        # get data from UniProt
        df = get_uniprot_results(genes)
        # build protein table
        df_prot = self._get_protein_df(df, genes)
        output.add(NodeTable(
            name='Proteins',
            df=df_prot,
            source_node='Gene',
            source_node_attr='gene',
            source_col=self.SOURCE_COL,
            node_label='Protein',
            id_attribute='Entry',
            attribute_cols=['name', 'Organism', 'ProteinNames', 'GeneNames', 'Function', 'uniProtUrl']
        ))

        # get GO (gene ontology) data from df
        if go:
            df_go = self._get_go_df(df)
            output.add(NodeTable(
                name='GO',
                df=df_go,
                source_node='Protein',
                source_col=self.SOURCE_COL,
                source_node_attr='Entry',
                node_label='GO',
                id_attribute='GoID',
                attribute_cols=['GoType', 'name']
            ))

        # get RefSeq data from df
        if refseq:
            pass

        return output

    def _get_protein_df(self, df: pd.DataFrame, genes: list) -> pd.DataFrame:
        """
        Method builds a pd.DataFrame with protein infos, based on UniProt response
        """
        # add source column
        if set(genes) != set(df[UNIPROT_COLS['gene_primary']['label']].values):
            for gene in genes:
                df.loc[df[UNIPROT_COLS['genes']['label']].str.lower().str.contains(gene.lower()), self.SOURCE_COL] = gene
        else:
            df = df.rename(columns={
                UNIPROT_COLS['gene_primary']['label']: self.SOURCE_COL})
        # shorten protein name -> get everything before first "("
        df['name'] = df[UNIPROT_COLS['protein_names']['label']].apply(lambda x: x.split('(')[0])
        # add node label
        df[self.NODEL_LABEL_COL] = 'Protein'
        # build uniProt url
        df['uniProtUrl'] = self.UNIPROT_URL + df[UNIPROT_COLS['id']['label']]
        # rename columns to replace spaces in names
        df = df.rename(columns={
            UNIPROT_COLS['cc_function']['label']: 'Function',
            UNIPROT_COLS['protein_names']['label']: 'ProteinNames',
            UNIPROT_COLS['genes']['label']: 'GeneNames'
        })
        # return df with specific columns
        return df[[
            self.SOURCE_COL,
            'Entry',
            self.NODEL_LABEL_COL,
            'name',
            'ProteinNames',
            'GeneNames',
            'Organism',
            'Function',
            'uniProtUrl'
        ]]

    def _get_go_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract GO information from uniProt df. As you can see in the example df below, all found GeneOntologies for
        each entry are stored as `;` seperated string. And each GeneOntology contains the string name and then the
        GO id in brackets (`[GO:002341]`).
        Therefore, this method runs through each entry and extracts the info.

        Note that we have three different gene ontology types:
        pymedgraph.UniProtPipe.GO_TYPES = ['molecular function', 'biological process', 'cellular component']

        :param df: pd.DataFrame - request response data from UniProt with the following structure
        example uniProt df:

                    |Entry  |prot name  |       Gene ontology (molecular function)      | ...   |
                    | --    |    ---    |               ---             ---             |  ...  |
                    | e1    | prot1     | go-Name1 [GO:0012]; go-Name2 [GO:0028]; ...   |  ...  |
                    | ..    |   ..      |               ..                              |  ...  |
        """
        source_entry = list()
        go_names = list()
        go_ids = list()
        go_types = list()
        # run through lines in df
        for go_type in self.GO_TYPES:
            col = f'Gene Ontology ({go_type})'
            for entry, go_list in zip(df['Entry'], df[col].fillna('')):
                try:
                    if go_list:
                        for go in go_list.split(';'):
                            go_split = go.split('[')
                            go_names.append(go_split[0].strip())
                            go_ids.append(go_split[1][:-1])
                            source_entry.append(entry)
                            go_types.append(go_type)
                except:
                    print('WARNING: unable to split GO list with value: ', go_list)
        # build df
        df = pd.DataFrame({
            self.SOURCE_COL: source_entry,
            'name': go_names,
            'GoID': go_ids,
            'GoType': go_types
        })
        # add node label
        df[self.NODEL_LABEL_COL] = 'GO'
        return df

    def _get_refseq_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Goal of this method is to build a node table df for RefSeq (Reference Sequence) data.
        This method works very similar to UniProtPipe._get_go_df() but only for the one RefSeq column.
        """
        source_entry = list()
        refseq_id = list()
        refseq_ids = list()
        pass