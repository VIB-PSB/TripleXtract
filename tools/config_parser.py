"""
Reads a config file and provides the parsed information as properties.
6 families of properties:
    1. database related
    2. NCBI taxonomy related
    3. gene related
    4. trait ontology related
    5. PubTator related
    6. miscellaneous
"""

import configparser
import os.path

from tools.arguments_parser import ArgumentsParser
from datetime import datetime
from tools.exceptions import ConfigError
from tools import tools

# pylint: disable=missing-function-docstring


class ConfigParser:
    """ Reads a config file and provides the parsed information as properties.
        12 families of properties, related to:
            1. database
            2. species NCBI taxonomy
            3. gene NCBI identifiers
            4. trait ontology
            5. gene ontology
            6. phenotype trait ontology
            7. gene2accession
            8. PLAZA
            9. PubTator
            10. statistics
            11. export
            12. miscellaneous
    """
    

    def __init__(self):
        self.parser = configparser.ConfigParser(inline_comment_prefixes="#")


    ####### DATABASE RELATED #######
    @property
    def db__name(self):
        return self.parser['database']['name']
    
    @property
    def db__host(self):
        return self.parser['database']['host']
    
    @property
    def db__user(self):
        return self.parser['database']['user']
    
    @property
    def db__password(self):
        return self.parser['database']['password']
    
    @property
    def db__purge(self):
        return self.parser.getboolean('database', 'purge')

    @property
    def db__purge_species_synonyms(self):  # if the user asks to import species synonyms to the database, the previous values have to be purged
        return self.parser.getboolean('species', 'import_to_db')

    @property
    def db__purge_gene_synonyms(self):  # if the user asks to import gene synonyms to the database, the previous values have to be purged
        return self.parser.getboolean('genes', 'import_to_db')

    @property
    def db__purge_trait_synonyms(self):  # if the user asks to import trait ontology to the database, the previous values have to be purged
        return self.parser.getboolean('trait_ontology', 'import_to_db')

    @property
    def db__purge_bp_synonyms(self):  # if the user asks to import gene ontology to the database, the previous values have to be purged
        return self.parser.getboolean('gene_ontology', 'import_to_db')

    @property
    def db__purge_ncbi_tables(self):  # if the user asks to import NCBI tables to the database, the previous values have to be purged
        return self.parser.getboolean('gene2accession', 'import_to_db')
    
    @property
    def db__purge_plaza_synonyms(self):  # if the user asks to import PLAZA synonyms to the database, the previous values have to be purged
        return self.parser.getboolean('plaza', 'import_plaza_synonyms')

    @property
    def db__purge_plaza_orthology(self):  # if the user asks to import PLAZA orthology to the database, the previous values have to be purged
        return self.parser.getboolean('plaza', 'import_orthology')



    ####### SPECIES RELATED #######
    @property
    def species__import_to_db(self):
        return self.parser.getboolean('species', 'import_to_db')

    @species__import_to_db.setter
    def species__import_to_db(self, value):
        self.parser['species']['import_to_db'] = value

    @property
    def species__url(self):
        return self.parser['species']['url']


    ####### GENES RELATED #######
    @property
    def genes__import_to_db(self):
        return self.parser.getboolean('genes', 'import_to_db')
    
    @genes__import_to_db.setter
    def genes__import_to_db(self, value):
        self.parser['genes']['import_to_db'] = value

    @property
    def genes__url(self):
        return self.parser['genes']['url']


    ####### TRAIT ONTOLOGY RELATED #######
    @property
    def to__import_to_db(self):
        return self.parser.getboolean('trait_ontology', 'import_to_db')
    
    @to__import_to_db.setter
    def to__import_to_db(self, value):
        self.parser['trait_ontology']['import_to_db'] = value

    @property
    def to__download_new_file(self):
        return self.parser.getboolean('trait_ontology', 'download_new_file')

    @property
    def to__url(self):
        return self.parser['trait_ontology']['url']

    @property
    def to__black_list_file_name(self):
        return self.parser['trait_ontology']['black_list_file_name']


    ####### GENE ONTOLOGY RELATED #######
    @property
    def go__import_to_db(self):
        return self.parser.getboolean('gene_ontology', 'import_to_db')
    
    @go__import_to_db.setter
    def go__import_to_db(self, value):
        self.parser['gene_ontology']['import_to_db'] = value

    @property
    def go__download_new_file(self):
        return self.parser.getboolean('gene_ontology', 'download_new_file')

    @property
    def go__url(self):
        return self.parser['gene_ontology']['url']

    @property
    def go__black_list_file_name(self):
        return self.parser['gene_ontology']['black_list_file_name']


    ####### PLANT PHENOTYPE AND TRAIT ONTOLOGY RELATED #######
    @property
    def ppto__import_to_db(self):
        return self.parser.getboolean('phenotype_trait_ontology', 'import_to_db')
    
    @ppto__import_to_db.setter
    def ppto__import_to_db(self, value):
        self.parser['phenotype_trait_ontology']['import_to_db'] = value

    @property
    def ppto__download_new_file(self):
        return self.parser.getboolean('phenotype_trait_ontology', 'download_new_file')

    @property
    def ppto__url(self):
        return self.parser['phenotype_trait_ontology']['url']

    @property
    def ppto__black_list_file_name(self):
        return self.parser['phenotype_trait_ontology']['black_list_file_name']


    ####### GENE2ACCESSION RELATED #######
    @property
    def g2a__import_to_db(self):
        return self.parser.getboolean('gene2accession', 'import_to_db')
    
    @g2a__import_to_db.setter
    def g2a__import_to_db(self, value):
        self.parser['gene2accession']['import_to_db'] = value

    @property
    def g2a__url(self):
        return self.parser['gene2accession']['url']


    ####### PLAZA RELATED #######
    @property
    def plaza__import_plaza_synonyms(self):
        return self.parser.getboolean('plaza', 'import_plaza_synonyms')
    
    @plaza__import_plaza_synonyms.setter
    def plaza__import_plaza_synonyms(self, value):
        self.parser['plaza']['import_plaza_synonyms'] = value
    
    @property
    def plaza__compute_links(self):
        return self.parser.getboolean('plaza', 'compute_links')
    
    @plaza__compute_links.setter
    def plaza__compute_links(self, value):
        self.parser['plaza']['compute_links'] = value

    @property
    def plaza__import_orthology(self):
        return self.parser.getboolean('plaza', 'import_orthology')

    @property
    def plaza__blast_dir(self):
        return self.parser['plaza']['blast_dir']

    @property
    def plaza__url_dicots(self):
        return self.parser['plaza']['url_dicots']
    
    @property
    def plaza__url_monocots(self):
        return self.parser['plaza']['url_monocots']
    
    @property
    def plaza__maize_mapping_url(self):
        return self.parser['plaza']['maize_mapping_url']
    
    @property
    def plaza__maize_mapping_v45_url(self):
        return self.parser['plaza']['maize_mapping_v45_url']

    @property
    def plaza__wheat_mapping_v11_file_name(self):
        return self.parser['plaza']['wheat_mapping_v1.1_file_name']

    @property
    def plaza__wheat_mapping_v21_file_name(self):
        return self.parser['plaza']['wheat_mapping_v2.1_file_name']
    
    @property
    def plaza__genome_links_file_name(self):
        return self.parser['plaza']['genome_links_file_name']

    @property
    def plaza__tree_based_orthology_url(self):
        return self.parser['plaza']['tree_based_orthology_url']

    @property
    def plaza__orthologous_gene_family_url(self):
        return self.parser['plaza']['orthologous_gene_family_url']

    @property
    def plaza__bhi_family_url(self):
        return self.parser['plaza']['bhi_family_url']

    @property
    def plaza__orthology_species_to_import(self):
        return self.parser['plaza']['orthology_species_to_import']

    @property
    def plaza__out_dir(self):
        return self.parser['plaza']['out_dir']


    ####### PUBTATOR #######
    @property
    def pubtator__import_to_db(self):
        return self.parser.getboolean('pubtator', 'import_to_db')
    
    @pubtator__import_to_db.setter
    def pubtator__import_to_db(self, value):
        self.parser['pubtator']['import_to_db'] = value

    @property
    def pubtator__file_name_pattern(self):
        return self.parser['pubtator']['file_name_pattern']

    @pubtator__file_name_pattern.setter
    def pubtator__file_name_pattern(self, value):
        self.parser['pubtator']['file_name_pattern'] = value

    @property
    def pubtator__start_doc_idx(self):
        return int(self.parser['pubtator']['start_doc_idx'])

    @pubtator__start_doc_idx.setter
    def pubtator__start_doc_idx(self, value):
        self.parser['pubtator']['start_doc_idx'] = str(value)

    @property
    def pubtator__end_doc_idx(self):
        return int(self.parser['pubtator']['end_doc_idx'])

    @pubtator__end_doc_idx.setter
    def pubtator__end_doc_idx(self, value):
        self.parser['pubtator']['end_doc_idx'] = str(value)

    @property
    def pubtator__species_synonyms_black_list_file_name(self):
        return self.parser['pubtator']['species_synonyms_black_list_file_name']

    @property
    def pubtator__gene_synonyms_black_list_file_name(self):
        return self.parser['pubtator']['gene_synonyms_black_list_file_name']


    ####### STATISTICS #######
    @property
    def stats__print_statistics(self):
        return self.parser.getboolean('stats', 'print_statistics')

    @property
    def stats__print_per_species_statistics(self):
        return self.parser.getboolean('stats', 'print_per_species_statistics')

    @property
    def stats__draw_all_plots(self):
        return self.parser.getboolean('stats', 'draw_all_plots')

    @property
    def stats__draw_max_score_vs_evidences(self):
        return self.parser.getboolean('stats', 'draw_max_score_vs_evidences')
    
    @property
    def stats__draw_bar_associations_per_case(self):
        return self.parser.getboolean('stats', 'draw_bar_associations_per_case')

    @property
    def stats__draw_bar_assoc_score_per_section_type(self):
        return self.parser.getboolean('stats', 'draw_bar_assoc_score_per_section_type')

    @property
    def stats__draw_bar_publications_per_year(self):
        return self.parser.getboolean('stats', 'draw_bar_publications_per_year')

    @property
    def stats__draw_bar_unique_triples_per_trait(self):
        return self.parser.getboolean('stats', 'draw_bar_unique_triples_per_trait')

    @property
    def stats__draw_hist_evidences_per_association(self):
        return self.parser.getboolean('stats', 'draw_hist_evidences_per_association')

    @property
    def stats__draw_hist_associations_per_paper(self):
        return self.parser.getboolean('stats', 'draw_hist_associations_per_paper')

    @property
    def stats__draw_hist_max_score_per_triple(self):
        return self.parser.getboolean('stats', 'draw_hist_max_score_per_triple')

    @property
    def stats__draw_hm_species_per_section(self):
        return self.parser.getboolean('stats', 'draw_hm_species_per_section')

    @property
    def stats__draw_hm_traits_per_species(self):
        return self.parser.getboolean('stats', 'draw_hm_traits_per_species')

    @property
    def stats__draw_hm_traits_per_species_selected(self):
        return self.parser.getboolean('stats', 'draw_hm_traits_per_species_selected')

    @property
    def stats__draw_hm_associations_per_paragraph_type(self):
        return self.parser.getboolean('stats', 'draw_hm_associations_per_paragraph_type')

    @property
    def stats__draw_upset_association_cases(self):
        return self.parser.getboolean('stats', 'draw_upset_association_cases')

    @property
    def stats__out_dir(self):
        return self.parser['stats']['out_dir']


    ####### DATA EXPORT #######
    @property
    def export__export_original_data(self):
        return self.parser.getboolean('export', 'export_original_data')
    
    @property
    def export__export_orthology_data(self):
        return self.parser.getboolean('export', 'export_orthology_data')

    @property
    def export__only_high_quality(self):
        return self.parser.getboolean('export', 'only_high_quality')
    
    @property
    def export__tm_min_occurrence_threshold(self):
        return self.parser['export']['tm_min_occurrence_threshold']
    
    @property
    def export__tm_min_occurrence_ortho_threshold(self):
        return self.parser['export']['tm_min_occurrence_ortho_threshold']
    
    @property
    def export__tm_max_score_threshold(self):
        return self.parser['export']['tm_max_score_threshold']
    
    @property
    def export__tm_max_score_ortho_threshold(self):
        return self.parser['export']['tm_max_score_ortho_threshold']

    @property
    def export__max_ortho_links(self):
        return self.parser['export']['max_ortho_links']

    @property
    def export__species_list_file_name(self):
        return self.parser['export']['species_list_file_name']

    @property
    def export__out_dir(self):
        return self.parser['export']['out_dir']


    ####### MISCELLANEOUS #######
    @property
    def misc__print_color_messages(self):
        return self.parser.getboolean('misc', 'print_color_messages')
    
    @property
    def misc__verbose(self):
        return self.parser.getboolean('misc', 'verbose')
    
    @property
    def misc__clear_tmp_files(self):
        return self.parser.getboolean('misc', 'clear_tmp_files')
    
    @misc__clear_tmp_files.setter
    def misc__clear_tmp_files(self, value):
        self.parser['misc']['clear_tmp_files'] = value

    @property
    def misc__out_dir(self):
        return self.parser['misc']['out_dir']
    
    @property
    def misc__tmp_dir(self):
        return self.parser['misc']['tmp_dir']


    def parse_config(self, config_file_name: str):
        """ Reads the provided config file and checks it.

        Parameters
        ----------
        config_file_name : str
            path to the config file

        Raises
        ------
        ConfigError
            raised for config-related errors
        """
        tools.print_info_message(f"PARSING CONFIG FILE '{config_file_name}'...", 0)
        config_file_name = os.path.abspath(config_file_name)
        if not os.path.isfile(config_file_name):
            raise ConfigError(f"The config file '{config_file_name}' doesn't exist.")
        self.parser.read(config_file_name)
        self._check_config()


    def parse_cli_arguments(self, arguments_parser: ArgumentsParser):
        """
        Parses arguments provided in command line.
        If an argument is defined, it overrides the value
        of the corresponding parameter in the config.

        Parameters
        ----------
        arguments_parser : ArgumentsParser
            object containing command line arguments provided by the user
        """
        if arguments_parser.import_species:
            self.species__import_to_db = arguments_parser.import_species
        if arguments_parser.import_genes:
            self.genes__import_to_db = arguments_parser.import_genes
        if arguments_parser.import_to:
            self.to__import_to_db = arguments_parser.import_to
        if arguments_parser.import_go:
            self.go__import_to_db = arguments_parser.import_go
        if arguments_parser.import_gene2acc:
            self.g2a__import_to_db = arguments_parser.import_gene2acc
        if arguments_parser.import_plaza_synonyms:
            self.plaza__import_plaza_synonyms = arguments_parser.import_plaza_synonyms
        if arguments_parser.compute_plaza_links:
            self.plaza__compute_links = arguments_parser.compute_plaza_links
        if arguments_parser.import_pubtator_annotations:
            self.pubtator__import_to_db = arguments_parser.import_pubtator_annotations
        if arguments_parser.pubtator_file_name_pattern is not None:
            self.pubtator__file_name_pattern = arguments_parser.pubtator_file_name_pattern
        if arguments_parser.pubtator_start_doc_idx is not None:
            self.pubtator__start_doc_idx = arguments_parser.pubtator_start_doc_idx
        if arguments_parser.pubtator_end_doc_idx is not None:
            self.pubtator__end_doc_idx = arguments_parser.pubtator_end_doc_idx
        if arguments_parser.clear_tmp:
            self.misc__clear_tmp_files = arguments_parser.clear_tmp


    def write_parameters(self, out_dir):
        """
        Writes the config to the specified directory.

        Parameters
        ----------
        out_dir : str
            output directory
        """
        out_file_name = os.path.join(out_dir, f"config__{datetime.now().strftime('%Y-%m-%d--%H-%M-%S')}.cfg")
        with open(out_file_name, "w", encoding="utf-8") as config_file:
            self.parser.write(config_file)
        tools.print_info_message(f"Parameters are stored in '{out_file_name}'.")


    def _check_config(self):
        """ Checks the validity of a config file.

        Raises
        ------
        ConfigError
            raised if problems were detected in the config file
        """
        assert self.species__import_to_db == self.db__purge_species_synonyms
        assert self.genes__import_to_db == self.db__purge_gene_synonyms
        assert self.to__import_to_db == self.db__purge_trait_synonyms 
        assert self.go__import_to_db == self.db__purge_bp_synonyms
        assert self.plaza__import_plaza_synonyms == self.db__purge_plaza_synonyms
        assert self.plaza__import_orthology == self.db__purge_plaza_orthology

        if self.to__import_to_db:
            if not os.path.isfile(self.to__black_list_file_name):
                raise ConfigError(f"The trait synonym black list file '{self.to__black_list_file_name}' doesn't exist.")
        if self.go__import_to_db:
            if not os.path.isfile(self.go__black_list_file_name):
                raise ConfigError(f"The biological process synonym black list file '{self.go__black_list_file_name}' doesn't exist.")
        if self.plaza__import_plaza_synonyms or self.plaza__compute_links or self.plaza__import_orthology:
            if not os.path.isfile(self.plaza__wheat_mapping_v11_file_name):
                raise ConfigError(f"The file with wheat genome mapping v11 '{self.plaza__wheat_mapping_v11_file_name}' doesn't exist.")
            if not os.path.isfile(self.plaza__wheat_mapping_v21_file_name):
                raise ConfigError(f"The file with wheat genome mapping v21 '{self.plaza__wheat_mapping_v21_file_name}' doesn't exist.")
        if self.plaza__compute_links:
            if not os.path.isfile(self.plaza__genome_links_file_name):
                raise ConfigError(f"The file with genome links '{self.plaza__genome_links_file_name}' doesn't exist.")
        if self.plaza__import_orthology:
            if not os.path.isfile(self.plaza__orthology_species_to_import):
                raise ConfigError(f"The file with species for which the orthology information should be imported '{self.plaza__orthology_species_to_import}' doesn't exist.")
        if self.pubtator__import_to_db:
            start_file_name = self.pubtator__file_name_pattern.replace("XXXXX", str(self.pubtator__start_doc_idx))
            if not os.path.isfile(start_file_name):
                raise ConfigError(f"The first file with PubTator annotations '{start_file_name}' doesn't exist.")
            if not os.path.isfile(self.pubtator__species_synonyms_black_list_file_name):
                raise ConfigError(f"The file with the black list of species synonyms '{self.pubtator__species_synonyms_black_list_file_name}' doesn't exist.")
            if not os.path.isfile(self.pubtator__gene_synonyms_black_list_file_name):
                raise ConfigError(f"The file with the black list of gene synonyms '{self.pubtator__gene_synonyms_black_list_file_name}' doesn't exist.")
        if (self.stats__draw_all_plots or 
            self.stats__draw_max_score_vs_evidences or
            self.stats__draw_bar_associations_per_case or 
            self.stats__draw_bar_assoc_score_per_section_type or
            self.stats__draw_bar_publications_per_year or
            self.stats__draw_bar_unique_triples_per_trait or
            self.stats__draw_hist_evidences_per_association or
            self.stats__draw_hist_associations_per_paper or
            self.stats__draw_hist_max_score_per_triple or
            self.stats__draw_hm_species_per_section or
            self.stats__draw_hm_traits_per_species or
            self.stats__draw_hm_traits_per_species_selected or
            self.stats__draw_hm_associations_per_paragraph_type or
            self.stats__draw_upset_association_cases):
            if not os.path.isdir(self.stats__out_dir):
                raise ConfigError(f"The statistics output directory '{self.stats__out_dir}' doesn't exist.")
        if self.export__export_original_data or self.export__export_orthology_data:
            if not os.path.isfile(self.export__species_list_file_name):
                raise ConfigError(f"The file with species list '{self.export__species_list_file_name}' doesn't exist.")
