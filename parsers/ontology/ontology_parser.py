"""
Abstract class allowing to parse ontology files.
Classes that inherit from it must implement _parse_ontology_file method.
"""

import errno
import networkx
import os

import obonet

from parsers.generic_parser import GenericParser
from tools import tools


class OntologyParser(GenericParser):
    """
    Abstract class allowing to parse ontology files.
    Classes that inherit from it must implement _parse_ontology_file method.
    """


    def __init__(self, ontology_name: str, ontology_url: str = "", synonym_black_list_file_name: str = "", tmp_dir: str = "", download_new_file: bool = True, verbose: bool = True):
        """
        Creates a dictionary of terms from the provided ontology.

        Parameters
        ----------
        ontology_name : str
            name of the ontology
        ontology_url : str, optional
            ontology URL, by default ""
        synonym_black_list_file_name : str, optional
            path to the file containing synonyms that should be ignored, by default ""
        tmp_dir : str, optional
            directory fort the downloaded ontology file, by default ""
        download_new_file : bool, optional
            indicates whether the file should be downloaded or retrieved from the temp directory, by default True
        verbose : bool, optional
             indicates whether detailed log messages should be printed, by default True
        """

        self.ontology_name = ontology_name
        self.ontology_url = ontology_url
        self.synonym_black_list_file_name = synonym_black_list_file_name
        self.tmp_dir = tmp_dir
        self.download_new_file = download_new_file

        self.term_length_threshold = 4  # only terms composed of 4 or more characters are taken into account
        
        self.black_list = set()
        self.ontology = None
        self.dictionary = {}

        GenericParser.__init__(self, verbose=verbose)


    def parse_files(self):
        """
        Parses an ontology file and creates a dictionary.
        Parsed terms are stored in a dictionary in the following format:
        dict[term_id] = [term_id | term_name | term_synonym_1 | term_synonym_2 | ...]
        Obsolete terms are discarded.
        Synonyms from the black list are discarded.
        """

        try:
            tools.print_info_message(f"PREPARING ONTOLOGY: {self.ontology_name}...", 0)
            self._parse_black_list_of_synonyms()
            self.ontology = self._get_ontology()
            self.dictionary = self._get_dictionary()

        except Exception as exception:
            tools.print_exception_message(f"Error occurred during ontology parsing: {exception}")


    def get_term_ancestors(self, term_id: str):
        """
        Provides a list of term ancestors.

        Parameters
        ----------
        term_id : str
            identifier of the term

        Returns
        -------
        List
            list of term ancestors
        """
        return networkx.descendants(self.ontology, term_id)


    def get_term_descendants(self, term_id: str):
        """
        Provides a list of term descendants.
        As networx library doesn't provide this functionality, we retrieve the list of all paths going to the provided term id.
        This information is given in form [{source_node, destination_node, relationship}].
        We only collect nodes having "is_a" relationship.

        Parameters
        ----------
        term_id : str
            identifier of the term

        Returns
        -------
        List
            list of term ancestors
        """
        result = []
        result.append(term_id)
        descendants = list(networkx.edge_dfs(self.ontology, term_id, orientation='reverse'))
        idx = 0
        while idx < len(result):
            a_term_id = result[idx]
            for descendant in descendants:
                if descendant[1] == a_term_id and descendant[2] == "is_a":
                    if descendant[0] not in result:
                        result.append(descendant[0])
            idx += 1
        result.remove(term_id)
        return result


    def _parse_black_list_of_synonyms(self):
        """
        Parses a file containing the black list of synonyms and stores them in self.black_list.
        """
        if self.synonym_black_list_file_name:
            with open(self.synonym_black_list_file_name, encoding="utf-8") as black_list_file:
                synonym_to_ignore = black_list_file.readline()
                while synonym_to_ignore:
                    self.black_list.add(synonym_to_ignore.strip().lower())
                    synonym_to_ignore = black_list_file.readline()

        if self._verbose:
            tools.print_info_message(f"Retrieved {len(self.black_list)} black listed synonyms.")
            
            
    def _get_ontology(self):
        """
        Parses the ontology file using the obonet library and returns the resulting graph object.

        Returns
        -------
        Ontology
            parsed ontology
        """
        file_name = os.path.join(self.tmp_dir, f"{self.ontology_name.lower()}.obo")

        if self.download_new_file:
            tools.print_info_message(f"Downloading the {self.ontology_name} file...")
            tools.download_url(self.ontology_url, file_name, self._verbose)
        elif not os.path.isfile(file_name):
            tools.print_warning_message(f"The ontology file {file_name} does not exist and should be downloaded first.")
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), file_name)
        else:
            tools.print_info_message(f"Using {self.ontology_name} file '{file_name}'...")
        
        graph = obonet.read_obo(self.ontology_url, ignore_obsolete=False)
        tools.print_info_message(f"Retrieved {len(graph):,} nodes and {graph.number_of_edges():,} edges.")
        return graph


    def _get_dictionary(self):
        """
        Creates a dictionary from the parsed ontology.
        The dictionary is in the following format:
        dict[term_id] = [term_id | term_name | term_synonym_1 | term_synonym_2 | ...]
        Synonyms from the black list are discarded.

        Returns
        -------
        Dict
            dictionary of terms
        """
        all_terms_cnt = 0
        retained_terms_cnt = 0
        synonyms_cnt = 0
        retained_synonyms_cnt = 0
        obs_terms_cnt = 0
        dictionary = {}

        # the first item of the term contains its identifier,
        # the second item contains all its properties
        for term in self.ontology.nodes(data=True):
            all_terms_cnt += 1
            if self._term_is_valid(term):
                term_id = term[0]
                retained_terms_cnt += 1
                if not 'is_obsolete' in term[1]:
                    dictionary[term_id] = term_id
                    synonyms = [term[1]['name']]
                    if 'synonym' in term[1]:
                        for synonym in term[1]['synonym']:
                            synonyms.append(synonym)
                    synonyms_cnt += len(synonyms)
                    for synonym in synonyms:
                        normalized_synonym = self._normalize_synonym(synonym)
                        if len(normalized_synonym) >= self.term_length_threshold and not normalized_synonym in self.black_list:
                            dictionary[term_id] += " | " + normalized_synonym
                            retained_synonyms_cnt += 1
                else:
                    obs_terms_cnt += 1
                    
        if self._verbose:
            tools.print_info_message(f"Terms -----> total: {all_terms_cnt:,} --- retained after filtering: {retained_terms_cnt:,} --- obsolete: {obs_terms_cnt:,}.")
            tools.print_info_message(f"Synonyms --> total: {synonyms_cnt:,} --- retained after filtering: {retained_synonyms_cnt:,}.")
            
        return dictionary


    def _term_is_valid(self, term):
        """
        Abstract method, must be implemented by inheriting classes to indicate the condition to keep the term.

        Raises
        ------
        NotImplementedError
            [description]
        """
        raise NotImplementedError


    def _normalize_synonym(self, synonym:str):
        """
        Normalize the provided synonym by removing useless suffixes, such as "related", "exact", "narrow"
        and transforming it to lowercase.

        Parameters
        ----------
        synonym : str
            synonym to normalize

        Returns
        -------
        str
            normalized synonym
        """
        result = synonym.strip()
        if '"' in result:
            result = result.split('"')[1]
        suffixes_to_remove = [" (related)", " (exact)", " (narrow)", " (broad)"]
        for suffix in suffixes_to_remove:
            if result.endswith(suffix):
                result = self._normalize_synonym(result[:-len(suffix)])  # recursive call because one synonym can have multiple suffixes
        suffixes_to_remove = [" trait", " traits"]
        for suffix in suffixes_to_remove:
            if result.endswith(suffix) and len(result.split(' ')) > 2:  # remove "trait(s)" from synonyms ending with "trait(s)", except if it only contains 2 words (ex: stress trait)
                result = result[:-len(suffix)]
        return result.lower()
