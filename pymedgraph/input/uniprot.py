import requests
from io import StringIO
import pandas as pd

from pymedgraph.dataextraction.uniprotcolumns import UNIPROT_COLS


UNIPROT_URL = 'https://rest.uniprot.org/uniprotkb/search'


def get_uniprot_entry(query: str, max_entries: int = 10, format_: str = "tab") -> pd.DataFrame:
    """ Get entries from Uniprot.

    :param query: Query for specific entries.
    :param max_entries: Number of Entries being fetched (Default=10).
    :param format_: Format of results.
    :returns: Returns reults of query in DataFrame format.
    """
    
    response = requests.get(
        UNIPROT_URL,
        params={
            "query": query,
            "limit": max_entries,
            "organism": 9606,
            "format": format_
        }
    )

    print(response.status_code)
    if response.status_code != 200:
        print("Error: ", response.status_code)

    return pd.read_csv(StringIO(response.text), delimiter='\t')


def get_uniprot_results(genes: list, extra_max_entries: int = 10, columns=None) -> pd.DataFrame:
    """
    Method to make request to UniProtKB and return result table as pd.DataFrame.
    
    :param genes: list - List of Genes.
    :param extra_max_entries: int - maximum number of entries.
    :param columns: Columns of the DataFrame (default=None)
    """
    if columns is None:
        columns = ','.join([v['returned_field'] for _, v in UNIPROT_COLS.items()])

    query = _build_query(genes, organism=True)
    response = requests.get(
        UNIPROT_URL,
        params={
            "query": query,
            "limit": len(genes) + extra_max_entries,
            'format': 'tsv',
            'fields': columns
        }
    )
    if response.status_code != 200:
        print("Error: ", response.status_code)
    # parse tab seperated table to pd.DataFrame
    return pd.read_csv(StringIO(response.text), delimiter='\t')


def _build_query(genes: list, organism=True, only_reviewed=True):
    """
    This method concatenates all genes in passed list to one query in order to minimize uniprot request.
    
    :param genes: list - List of Genes.
    :param organism: Organism the query should be based on (default = Homo sapiens)
    :param only_reviewd: check if query was reviewed (default = True)
    """
    query = '(' + ' OR '.join(['gene:' + g for g in genes]) + ')'
    # filter for only reviewed entries
    if only_reviewed:
        query =  query + ' AND reviewed:true'
    # filter entries for humans only
    if organism:
        query = query + ' AND organism_id:9606'
    return query
