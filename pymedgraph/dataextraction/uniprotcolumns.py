"""
Names of columns and returned fields for uniport requests.
Might be updated in the future

source: https://www.uniprot.org/help/return_fields
"""

UNIPROT_COLS = {
    'id': {
        'label': 'Entry',
        'legacy_returned_field': 'id',
        'returned_field': 'accession'
    },
    'entry_name': {
        'label': 'Entry name',
        'legacy_returned_field': 'entry name',
        'returned_field': 'id'
    },
    'reviewed': {
        'label': 'Reviewed',
        'legacy_returned_field': 'reviewed',
        'returned_field': 'reviewed'
    },
    'protein_names': {
        'label': 'Protein names',
        'legacy_returned_field': 'protein names',
        'returned_field': 'protein_name'
    },
    'genes': {
        'label': 'Gene Names',
        'legacy_returned_field': 'genes',
        'returned_field': 'gene_names'
    },
    'gene_primary': {
        'label': 'Gene Names (primary)',
        'legacy_returned_field': 'genes(PREFERRED)',
        'returned_field': 'gene_primary'
    },
    'organism': {
        'label': 'Organism',
        'legacy_returned_field': 'organism',
        'returned_field': 'organism_name'
    },
    'cc_function': {
        'label': 'Function [CC]',
        'legacy_returned_field': 'comment(FUNCTION)',
        'returned_field': 'cc_function'
    },
    'refseq': {
        'label': 'RefSeq',
        'legacy_returned_field': 'database(RefSeq)',
        'returned_field': 'xref_refseq'
    },
    'go_biological': {
        'label': 'Gene Ontology (biological process)',
        'legacy_returned_field': 'go(biological process)',
        'returned_field': 'go_p'
    },
    'go_cellular': {
        'label': 'Gene Ontology (cellular component)',
        'legacy_returned_field': 'go(cellular component)',
        'returned_field': 'go_c'
    },
    'go_molecular': {
        'label': 'Gene Ontology (molecular function)',
        'legacy_returned_field': 'go(molecular function)',
        'returned_field': 'go_f'
    }
}
