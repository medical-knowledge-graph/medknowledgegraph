import requests
from io import StringIO
import pandas as pd

UNIPROT_URL = "https://www.uniprot.org/uniprot/"


def get_uniprot_entry(query: str, max_entries: int = 10, format_: str = "tab") -> pd.DataFrame:
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
    """
    if columns is None:
        columns = 'id,entry name,reviewed,protein names,genes,genes(PREFERRED),organism,comment(FUNCTION),' \
                  'database(RefSeq),go(molecular function),go(biological process),go(cellular component)'

    query = _build_query(genes, organism=True)
    response = requests.get(
        UNIPROT_URL,
        params={
            "query": query,
            "limit": len(genes) + extra_max_entries,
            'format': 'tab',
            'columns': columns
        }
    )
    if response.status_code != 200:
        print("Error: ", response.status_code)
    # parse tab seperated table to pd.DataFrame
    return pd.read_csv(StringIO(response.text), delimiter='\t')


def _build_query(genes: list, organism=True, only_reviewd=True):
    """
    This method concatenates all genes in passed list to one query in order to minimize uniprot request.
    """
    query = '(' + ' OR '.join(['gene:' + g for g in genes]) + ')'
    # filter for only reviewed entries
    if only_reviewd:
        query =  query + ' AND reviewed:yes'
    # filter entries for humans only
    if organism:
        query = query + ' AND organism:"Homo sapiens (Human) [9606]"'
    return query
