"""
Parses the gene ontology and extracts biological processes names and synonyms.
"""

from parsers.ontology.ontology_parser import OntologyParser


class GeneOntologyParser(OntologyParser):
    """
    Parses gene ontology and extracts biological processes names and synonyms.
    """


    def __init__(self, go_url: str = "", bp_synonym_black_list_file_name: str = "", tmp_dir: str = "", download_new_file: bool = True, verbose: bool = True):
        OntologyParser.__init__(self, "GO", go_url, bp_synonym_black_list_file_name, tmp_dir, download_new_file, verbose)


    def _term_is_valid(self, term):
        return term[1]['namespace'] == 'biological_process'
