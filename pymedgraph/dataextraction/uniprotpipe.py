import pandas as pd
from pymedgraph.dataextraction.basepipe import BasePipe, PipeOutput, NodeTable
from pymedgraph.input.uniprot import get_uniprot_entry


class UniProtPipe(BasePipe):
    """
    |source |node label  | $attr$Name  | $attr$UniProtID    |
    |GenXY  | ProteinXY  | geneName     | xyIDxy            |
    """
    def __init__(self):
        super().__init__(pipe_name='UniProtPipe')

    def _run_pipe(self, genes: list) -> PipeOutput:
        output = PipeOutput(self.name)

        all_proteins = list()
        for gene in genes:
            df = get_proteins_for_gene(gene)
            df.insert(loc=0, column="source", value=gene)
            all_proteins.append(df)

        all_proteins_dataframe = pd.concat(all_proteins)
        all_proteins_dataframe.insert(loc=0, column="node_label", value="Protein")

        output.add(NodeTable(
            name='Proteins',
            df=all_proteins_dataframe,
            source_node='Gene',
            source_node_attr='gene',
            source_col=self.SOURCE_COL,
            node_label='Protein',
            id_attribute='Protein names',
            attribute_cols=['Entry', 'Organism']
        ))

        return output


def get_proteins_for_gene(gene: str) -> pd.DataFrame:
    single_protein_dataframe = get_uniprot_entry(gene, [])
    # Allow only Status = reviewed
    single_protein_dataframe = single_protein_dataframe[single_protein_dataframe["Status"] == 'reviewed']
    single_protein_dataframe = single_protein_dataframe[single_protein_dataframe["Organism"]
        .str.lower().str.contains('human')]
    return single_protein_dataframe
