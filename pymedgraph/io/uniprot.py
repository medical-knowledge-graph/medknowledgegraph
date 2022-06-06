import requests

UNIPROT_URL = "https://www.uniprot.org/uniprot/"

def get_uniprot_entry(query: str, columns:list, max_entries: int = 1, format_: str = "tab") -> dict:
    # TODO: get Protein
    if not columns:
        columns
    response = requests.get(
        UNIPROT_URL,
        params={
            "query": query,
            "limit": max_entries,
            "organism": 9606,
            "format": format_
        }
    )
