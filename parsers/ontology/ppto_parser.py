"""
Parses the plant phenotype and trait ontology and extracts phenotype and trait names and synonyms.
"""
import errno
import obonet
import os

from parsers.ontology.ontology_parser import OntologyParser
from tools import tools


class PlantPhenotypeTraitOntologyParser(OntologyParser):
    """
    Parses the plant phenotype and trait ontology and extracts phenotype and trait synonyms.
    """


    def __init__(self, ppto_url: str = "", trait_synonym_black_list_file_name: str = "", tmp_dir: str = "", download_new_file: bool = True, verbose: bool = True):
        self.ppto_to_to_dict = {}
        OntologyParser.__init__(self, "PPTO", ppto_url, trait_synonym_black_list_file_name, tmp_dir, download_new_file, verbose)


    def _term_is_valid(self, term):
        return term[0][:5] == 'PPTO:'


    def _get_ontology(self):
        """
        Overrides the parent method: as PPTO ontology uses some special characters that
        cannot be parsed, it is first downloaded (if asked by user), encoded to ANSII and then parsed.

        Parameters
        ----------
        ppto_url : str
            URL of the PPTO

        Returns
        -------
        Dict
            PPTO dictionary
        """
        file_name = os.path.join(self.tmp_dir, "ppto.obo")

        if self.download_new_file:
            tools.print_info_message(f"Downloading and patching {self.ontology_name} file...")
            tools.download_and_extract_gz_file(self.ontology_url, file_name, self._verbose)
            tools.convert_file_encoding(file_name, "utf-8", "ascii")
        elif not os.path.isfile(file_name):
            tools.print_warning_message(f"The PPTO file {file_name} does not exist and should be downloaded first.")
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), file_name)
        else:
            tools.print_info_message(f"Using the {self.ontology_name} file {file_name}...")
        
        graph = obonet.read_obo(file_name, ignore_obsolete=False)
        tools.print_info_message(f"Retrieved {len(graph):,} nodes and {graph.number_of_edges():,} edges.")

        return graph
        

    def _get_dictionary(self):
        """
        Calls the parent method to compute the dictionary of synonyms for each trait.
        In addition, creates a dictionary associating PPTO term ids to TO term ids, in form:
        dict[PPTO_id] = [TO_id_1, TO_id_2, ...]

        Returns
        -------
        Dict
            dictionary of PPTO synonyms
        """
        # the first item of the term contains its identifier,
        # the second item contains all its properties
        for term in self.ontology.nodes(data=True):
            definition = term[1]['def']
            if '(TO:' in definition:
                term_id = term[0]
                line_parts = definition.split(' ')
                self.ppto_to_to_dict[term_id] = [item[item.find('(')+1:].strip('();:,."\n ') for item in line_parts if '(TO:' in item]
                self.ppto_to_to_dict[term_id] = [item for item in self.ppto_to_to_dict[term_id] if item != "TO:New"]  # a patch to remove "TO:New" value
        return OntologyParser._get_dictionary(self)
