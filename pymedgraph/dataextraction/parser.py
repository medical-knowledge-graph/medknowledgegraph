def get_pubmed_id(paper: dict, sep: str = '~') -> str:
    if 'PubmedData' not in paper.keys():
        raise RuntimeError('Got unexpected Pubmed paper dict to retrieve id from:', paper)
    # get list
    id_list = paper.get('PubmedData').get('ArticleIdList')
    # check
    if not id_list:
        raise RuntimeError('Failed to get pubmed id from paper:', paper)
    if len(id_list) > 1:
        # TODO: make sure to get pubmed id
        print('WARNING: Found multiple Ids for paper. Might be ignored.', id_list)
    if id_list[0].attributes['IdType'] != 'pubmed':
        print('WARNING: Unexpected `IdType` in IdList. Expected is `pubmed` but found `{it_}`.'.format(
            it_=id_list[0].attributes['IdType']
        ))

    # build uri
    paper_id = id_list[0].attributes['IdType'] + sep + id_list[0]

    return paper_id

def parse_pubmed_article(paper: dict) -> str:
    """ get article and join different sections of abstract to one """
    abstract_sections = paper['MedlineCitation']['Article']['Abstract']['AbstractText']
    return ' '.join(section.title() for section in abstract_sections)


def get_pubmed_title(paper: dict) -> str:
    return paper['MedlineCitation']['Article']['ArticleTitle']
