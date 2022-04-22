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
