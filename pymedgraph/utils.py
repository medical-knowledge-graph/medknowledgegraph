import requests


def store_medgen_genes_set(file_path: str, medgen_column=None):
    """
    Method to download complete list of Gene-MedGen relationships, as they state in the documentation:
    https://www.ncbi.nlm.nih.gov/medgen/docs/help/#eutilities-and-entrez-direct

    :param file_path: str - path to file, where set of genes is going to be stored
    :param medgen_column: str - name of MedGen CUI column in list, if None method is looking for column
    """
    gene_cuis = list()

    # get data
    URL = 'https://ftp.ncbi.nih.gov/gene/DATA/mim2gene_medgen'
    result = requests.get(URL)
    # transform to lines
    table_rows = result.text.split('\n')
    # set column head
    table_heads = [c for c in table_rows.pop(0).split('\t')]
    if medgen_column is None:
        try:
            medgen_column = [c for c in table_heads if 'cui' in c.lower()][0]
        except IndexError:
            raise RuntimeError(f'Cannot find suitable CUI column in {table_heads}. Please set medgen_column parameter.')
    col_indx = table_heads.index(medgen_column)

    # build list
    for row in table_rows:
        if row:
            row_cols = row.split('\t')
            if row_cols and row_cols[col_indx][0] == 'C':
                gene_cuis.append(row_cols[col_indx])
            # gene_cuis = [r.split('\t')[col_indx] for r in table_rows if r]

    # write list to file
    with open(file_path, 'w') as fh:
        fh.write('\n'.join(gene_cuis))
    return f'Stored {len(gene_cuis)} elements under {file_path}.'
