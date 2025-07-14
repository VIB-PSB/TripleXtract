"""
Parses NCBI taxonomy files and creates the corresponding plant species dictionary.
Taxonomy FTP: https://ftp.ncbi.nih.gov/pub/taxonomy/
File to donwload: taxdump.tar.gz
"""

import csv
import os

from parsers.generic_parser import GenericParser
from tools import tools


class NcbiPlantTaxonomyParser(GenericParser):
    """
    Parses NCBI taxonomy files and extracts a plant species dictionary.
    Taxonomy FTP: https://ftp.ncbi.nih.gov/pub/taxonomy/
    File to donwload: taxdump.tar.gz
    """


    def __init__(self, taxonomy_url: str, out_dir: str, verbose: bool = True):
        """
        Parses the provided URL and creates the NCBI plant species dictionary.
        The dictionary is in form dict[id] = list[synonym].

        Parameters
        ----------
        taxonomy_url : str
            URL for taxonomy files, by default ""
        out_dir : str
            output directory for downloaded files
        verbose: bool, optional
            indicates when log messages should be printed; by default True
        """

        self.taxonomy_url = taxonomy_url
        self.out_dir = out_dir
        self.ncbi_names_dict = {}
        self.ncbi_nodes_dict = {}
        self.plant_kingdom_root = {}

        GenericParser.__init__(self, verbose=verbose)


    def parse_files(self):
        """
        Parses NCBI names file and NCBI nodes file and constructs the corresponding plant dictionary.
        """
        try:
            tools.print_info_message("IMPORTING PLANT TAXONOMY...", 0)
            folder_name = os.path.join(self.out_dir, "taxdump")
            tools.download_and_extract_targz_folder(self.taxonomy_url, folder_name, True)
            self._parse_ncbi_names(f"{folder_name}/names.dmp")
            self._parse_ncbi_nodes(f"{folder_name}/nodes.dmp")

            if self._verbose:
                tools.print_info_message("Constructing plant dictionary...")
            self._build_tree_and_plant_dict(self.plant_kingdom_root)
            if self._verbose:
                tools.print_info_message(f"Retrieved {len(self.dictionary):,} plant species.", 2)
        except Exception as exception:
            tools.print_exception_message(f"NCBI taxonomy could not be parsed: {exception}")


    def _parse_ncbi_names(self, ncbi_names_file_name: str):
        """
        Parses the provided NCBI file names.dmp and creates names dictionary:
        ncbi_names_dict['ncbi_id'] = name | synonym 1 | synonym 2 | etc

        Parameters
        ----------
        ncbi_names_file_name : str
            name of the file with the dump of NCBI names
        """
        try:
            if self._verbose:
                tools.print_info_message(f"Parsing NCBI names file '{ncbi_names_file_name}'...")
            item_cnt = 0
            with open(ncbi_names_file_name, encoding="utf8") as ncbi_names_file:
                reader = csv.reader(ncbi_names_file, delimiter="|")
                for row in reader:
                    item_cnt += 1
                    key = row[0].strip()
                    if key not in self.ncbi_names_dict:  # the first occurrence
                        self.ncbi_names_dict[key] = row[1].strip()
                    else:  # a synonym - added to the name after a "|" symbol
                        self.ncbi_names_dict[key] += " | " + row[1].strip()
                    if self._verbose and item_cnt % 1_000_000 == 0:
                        tools.print_info_message(f"Parsed {item_cnt:,} species names so far...", 2)
            if self._verbose:
                tools.print_info_message(f"Parsed {item_cnt:,} species names.", 2)
        except Exception as exception:
            tools.print_exception_message(f"NCBI names file could not be parsed: {exception}")


    def _parse_ncbi_nodes(self, ncbi_nodes_file_name: str):
        """
        Parses the provided NCBI nodes.dmp file in the format "tax_id | parent_tax_id | rank | division_id | other info".
        Since we are only interested in plants, only elements with division_id = 4 are selected (plants & fungi).
        The selected elements will then be used to recreate the tree corresponding to the NCBI sub-tree for Viridiplantae.
        For that purpose, the root node is retained (with tax_id = 33090).
        To facilitate the tree construction, a node dictionary is constructed in the following format:
        node_dict[parent_tax_id] = [list of children of the node with tax_id = parent_tax_id]

        Parameters
        ----------
        ncbi_nodes_file_name : str
            name of the file with the dump of NCBI nodes
        """
        try:
            if self._verbose:
                tools.print_info_message(f"Parsing NCBI nodes file '{ncbi_nodes_file_name}'...")
            item_cnt = 0
            with open(ncbi_nodes_file_name, newline='', encoding="utf8") as ncbi_nodes_file:
                reader = csv.reader(ncbi_nodes_file, delimiter="|")
                for row in reader:
                    tax_id = row[0].strip()
                    parent_tax_id = row[1].strip()
                    rank = row[2].strip()
                    division_id = int(row[4].strip())
                    if division_id == 4:
                        item_cnt += 1
                        self.ncbi_nodes_dict.setdefault(parent_tax_id, [])
                        self.ncbi_nodes_dict[parent_tax_id].append({ 'tax_id':tax_id, 'rank':rank })
                        if tax_id == '33090': # Viridiplantae
                            self.plant_kingdom_root = { 'tax_id':tax_id, 'rank':rank }  # stored here as it will be the root of the tree
                        if self._verbose and item_cnt % 1_000_000 == 0:
                            tools.print_info_message(f"Parsed {item_cnt:,} names so far...", 2)
            if self._verbose:
                tools.print_info_message(f"Parsed {item_cnt:,} plant and fungi nodes.", 2)
        except Exception as exception:
            tools.print_exception_message(f"NCBI nodes file could not be parsed: {exception}")


    def _build_tree_and_plant_dict(self, node: dict):
        """
        Recursively computes the plant dictionary from the previously build dictionaries:
        - names dictionary
        - nodes dictionary
        Nodes dictionary is used to reconstruct the tree structure, from the provided root node.

        Parameters
        ----------
        node : dict
            current node of the tree
        """
        if node['tax_id'] in self.ncbi_nodes_dict:
            children = self.ncbi_nodes_dict[node['tax_id']]
            if children:
                for child in children:
                    self.dictionary[child['tax_id']] = self.ncbi_names_dict[child['tax_id']]
                    self._build_tree_and_plant_dict(child)
