import pandas as pd
from pymedgraph.dataextraction.basepipe import BasePipe, PipeOutput, NodeTable
from pymedgraph.input.uniprot import get_uniprot_entry, get_uniprot_results


class UniProtPipe(BasePipe):
    """
    |source |node label  | $attr$Name  | $attr$UniProtID    |
    |GenXY  | ProteinXY  | geneName     | xyIDxy            |
    """

    UNIPROT_URL = 'https://www.uniprot.org/uniprot/'
    GO_TYPES = ['molecular function', 'biological process', 'cellular component']

    def __init__(self):
        super().__init__(pipe_name='UniProtPipe')

    def _run_pipe(self, genes: list, go=True, mrna=True) -> PipeOutput:
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
            attribute_cols=['Entry', 'name', 'Organism', 'Protein names', 'Gene names', 'Function [CC]', 'uniProtUrl']
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
                attribute_cols=['GoID', 'Go-type', 'name']
            ))

        # get mRNA RefSeq data from df
        if mrna:
            pass

        return output

    def _get_protein_df(self, df: pd.DataFrame, genes: list) -> pd.DataFrame:
        # add source column
        if set(genes) != set(df['Gene names  (primary )'].values):
            for gene in genes:
                df.loc[df['Gene names'].str.lower().str.contains(gene.lower()), self.SOURCE_COL] = gene
        else:
            df = df.rename(columns={'Gene names  (primary )': self.SOURCE_COL})
        # shorten protein name -> get everything before first "("
        df['name'] = df['Protein names'].apply(lambda x: x.split('(')[0])
        # add node label
        df[self.NODEL_LABEL_COL] = 'Protein'
        # build uniProt url
        df['uniProtUrl'] = self.UNIPROT_URL + df['Entry']
        # return df with specific columns
        return df[[
            self.SOURCE_COL,
            'Entry',
            self.NODEL_LABEL_COL,
            'name',
            'Protein names',
            'Gene names',
            'Organism',
            'Function [CC]'
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
            col = f'Gene ontology ({go_type})'
            for entry, go_list in zip(df['Entry'], df[col]):
                for go in go_list.split(';'):
                    go_split = go.split('[')
                    go_names.append(go_split[0].strip())
                    go_ids.append(go_split[1][:-1])
                    source_entry.append(entry)
                    go_types.append(go_type)
        # build df
        df = pd.DataFrame({
            self.SOURCE_COL: source_entry,
            'name': go_names,
            'GoID': go_ids,
            'Go-type': go_types
        })
        # add node label
        df[self.NODEL_LABEL_COL] = 'GO'
        return df