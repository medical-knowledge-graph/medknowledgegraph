from Bio import Entrez


class NCBIFetcher(object):
    def __init__(self, email, tool_name, max_articles=10):
        Entrez.email = email
        Entrez.tool = tool_name
        self.max_articles = max_articles

    def get_pubmed_paper(self, term: str, n_articles: int = None) -> list:
        """ method returns articles only. Books are not supported yet """
        paper_ids = self.search_pubmed(term, n_articles)
        # do request
        handle = Entrez.efetch(db='pubmed', id=paper_ids, retmode='xml')
        records = Entrez.read(handle)
        handle.close()
        return records['PubmedArticle']

    def search_pubmed(self, term: str, n_articles: int = None) -> list:
        """
        method to first search for term in pubmed and do another fetch with received IDs
        """
        # make sure to not fetch more articles then set `max_articles`
        if not n_articles or n_articles > self.max_articles:
            n_articles = self.max_articles
        # do request
        handle = Entrez.esearch(db='pubmed', term=term, idtype='acc', retmax=n_articles, sort="relevance")
        # parse response
        record = Entrez.read(handle)
        handle.close()
        # get IDs of found articles
        article_ids = record['IdList']
        return article_ids
