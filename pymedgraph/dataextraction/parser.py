def get_pubmed_id(paper: dict, sep: str = '~') -> str:
    if 'PubmedData' not in paper.keys():
        raise RuntimeError('Got unexpected Pubmed paper dict to retrieve id from:', paper)
    indx = 0
    # get list
    id_list = paper.get('PubmedData').get('ArticleIdList')
    # check
    if not id_list:
        raise RuntimeError('Failed to get pubmed id from paper:', paper)
    # if multiple ids exist make sure to get pubmed
    if len(id_list) > 1 and id_list[indx].attributes['IdType'] != 'pubmed':
        for ix, id_ in id_list:
            if id_ == 'pubmed':
                indx = ix
                break
        if id_list[indx].attributes['IdType'] != 'pubmed':
            raise RuntimeError(f'Cannot find `pubmed` id in id_list: {id_list}')
    # build uri
    paper_id = id_list[indx].attributes['IdType'] + sep + id_list[indx]

    return paper_id

def parse_pubmed_article(paper: dict) -> str:
    """ get article and join different sections of abstract to one """
    try:
        abstract_sections = paper['MedlineCitation']['Article']['Abstract']['AbstractText']
        abstract = ' '.join(section for section in abstract_sections)
    except KeyError:
        print('WARNING: Found no Abstract in ', paper['MedlineCitation'])
        abstract = ''
    return abstract


def get_pubmed_title(paper: dict) -> str:
    return paper['MedlineCitation']['Article']['ArticleTitle']
