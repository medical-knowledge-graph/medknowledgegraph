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
        return

    return pd.read_csv(StringIO(response.text), delimiter='\t')
