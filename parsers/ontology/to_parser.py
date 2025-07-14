"""
Parses the trait ontology and extracts trait names and synonyms.
"""

from parsers.ontology.ontology_parser import OntologyParser


class TraitOntologyParser(OntologyParser):
    """
    Parses the trait ontology and extracts trait synonyms.
    """


    def __init__(self, to_url: str = "", trait_synonym_black_list_file_name: str = "", tmp_dir: str = "", download_new_file: bool = True, verbose: bool = True):
        OntologyParser.__init__(self, "TO", to_url, trait_synonym_black_list_file_name, tmp_dir, download_new_file, verbose)


    def _term_is_valid(self, term):
        return term[0][:3] == 'TO:'


    def get_term_ancestors(self, term_id: str):
        """
        Overwrites the parental method, in order to filter results.
        In trait ontology, several term types exist (for example: TO, BFO, PATO, ...)
        Here, we are only interested in TO terms.

        Parameters
        ----------
        term_id : str
            trait identifier

        Returns
        -------
        List
            filtered list of term ancestors
        """
        ancestors = OntologyParser.get_term_ancestors(self, term_id)
        result = [item for item in ancestors if item.startswith("TO:")]
        return result


    def get_term_descendants(self, term_id: str):
        """
        Overwrites the parental method, in order to filter results.
        In trait ontology, several term types exist (for example: TO, BFO, PATO, ...)
        Here, we are only interested in TO terms.

        Parameters
        ----------
        term_id : str
            trait identifier

        Returns
        -------
        List
            filtered list of term ancestors
        """
        ancestors = OntologyParser.get_term_descendants(self, term_id)
        result = [item for item in ancestors if item.startswith("TO:")]
        return result
