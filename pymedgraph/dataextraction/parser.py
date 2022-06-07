import xml.etree.ElementTree as ET


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


def get_mash_terms(paper):
    mesh_list_name = 'MeshHeadingList'
    found_terms = list()

    # check if exists
    if mesh_list_name not in paper['MedlineCitation'].keys():
        print(RuntimeWarning('Found no mesh terms for paper'))
    else:
        # extract terms
        try:
            found_terms += [mesh_term['DescriptorName'].title() for mesh_term in paper['MedlineCitation'][mesh_list_name]]
        except KeyError:
            raise RuntimeWarning(f'Cannot find DescriptorName in {mesh_list_name}:',
                                 paper['MedlineCitation'][mesh_list_name])
    return found_terms


def get_keywords(paper: dict) -> list:
    """ Extract strings from keyword list of paper"""
    found_words = list()
    try:
        keyword_lists = paper['MedlineCitation']['KeywordList']
        for kw_list in keyword_lists:
            found_words += [s.title() for s in kw_list]
    except KeyError:
        raise RuntimeWarning('Cannot find KeywordList in paper ', paper)

    return found_words


def parse_medgen(xml_records, snomed=False, clinical_features=False):
    """ parse Entrez XML Response """
    summaries = dict()
    root = ET.fromstring(xml_records)
    if not root.tag == 'eSummaryResult':
        raise RuntimeError('Unexpected XML was passed.')
    # build dictionary for each summary
    for summary in root.findall('DocumentSummarySet/DocumentSummary'):
        # get uid
        uid = summary.attrib['uid']
        # build dict
        summary_dict = dict()
        summary_dict['cui'] = summary.find('ConceptId').text
        summary_dict['genes'] = [g.text for g in summary.findall('ConceptMeta/AssociatedGenes/Gene')]
        if snomed:
            summary_dict['snomed'] = {
                sc.attrib['SAUI']: {
                    'text': sc.text,
                    'SCUI': sc.attrib['SCUI'],
                    'SAB': sc.attrib['SAB']
                } for sc in summary.findall('ConceptMeta/SNOMEDCT/Name')
            }
        if clinical_features:
            summary_dict['clinical_features'] = {
                cf.attrib['CUI']: {
                    'type': cf.find('SemanticType').text if cf.find('SemanticType') is not None else '',
                    'name': cf.find('Name').text if cf.find('Name') is not None else '',
                    'definition': cf.find('Definition').text if cf.find('Definition') is not None else ''
                } for cf in summary.findall('ConceptMeta/ClinicalFeatures/ClinicalFeature')
            }
        # add dict to results
        summaries[uid] = summary_dict
    return summaries
