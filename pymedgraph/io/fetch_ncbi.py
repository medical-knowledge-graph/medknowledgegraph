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
        method to get UID`s from pubmed for given search term. The UID`s are used to fetch the pubmed records
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

    def get_medgen_summaries(self, cui:list):
        uids = self.get_medgen_uids(cui)
        handle = Entrez.esummary(db='medgen', id=','.join(uids), retmode='xml')
        # TODO parse records, Entrez.read(handle) does not work!
        return handle

    def get_medgen_uids(self, cui:list) -> list:
        """
        We search UID`s in the NCBI MedGen database with concept IDs (CUI) which we got from the UMLS linkage.
        The UID`s will be used to get the MedGen summary for the found concept.
        """
        # to make only one request for multiple CUI concepts we join the ids with an OR
        search_term = ' OR '.join(cui)
        handle = Entrez.esearch(db='medgen', term=search_term)
        record = Entrez.read(handle)
        handle.close()
        return record['IdList']
