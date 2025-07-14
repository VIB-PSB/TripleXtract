"""
Parses an NCBI taxonomy, NCBI gene identifiers and synonyms, a trait ontology and PubTator Annotations.
Creates triples (species, gene, trait).
"""


from datetime import datetime
import os
import shutil
import semantic_version
import time

from export.data_exporter import DataExporter
from export.stats_extractor import StatsExtractor
from parsers.ncbi.gene_identifier_parser import NcbiGeneIdentifierParser
from parsers.ncbi.plant_taxonomy_parser import NcbiPlantTaxonomyParser
from parsers.ontology.to_parser import TraitOntologyParser
from parsers.ontology.go_parser import GeneOntologyParser
from parsers.ontology.ppto_parser import PlantPhenotypeTraitOntologyParser
from plaza.ncbi_to_plaza_linker import NcbiToPlazaLinker
from plaza.orthology_handler import OrthologyHandler
from pubtator.pt_parser import PubTatorParser
from tools import tools
from tools.arguments_parser import ArgumentsParser
from tools.config_parser import ConfigParser
from tools.database_handler import DatabaseHandler
from tools.ontology_term_propagator import OntologyTermPropagator

os.environ['OPENBLAS_NUM_THREADS'] = '1'  # prevents threads creation on clusters


def main():
    """
    Program entry point
    """
    try:
        version = semantic_version.Version('1.0.0')

        tools.print_info_message(f"TripleXtract v. {version} <===", 0)

        tools.print_info_message(f"Pipeline started on: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        start_time = time.time()

        arguments_parser = ArgumentsParser()
        arguments_parser.parse_arguments()

        config_parser = ConfigParser()
        config_parser.parse_config(arguments_parser.config_file_name)
        config_parser.parse_cli_arguments(arguments_parser) # the parameters provided in command line override the parameters from the config file

        # write the parsed parameters to a file
        config_parser.write_parameters(config_parser.misc__out_dir)

        tools.COLORS_ENABLED = config_parser.misc__print_color_messages
        verbose = config_parser.misc__verbose

        db_handler = DatabaseHandler(config_parser.db__name, config_parser.db__host, config_parser.db__user, config_parser.db__password, config_parser)
        to_parser = TraitOntologyParser(config_parser.to__url, config_parser.to__black_list_file_name, config_parser.misc__tmp_dir, config_parser.to__download_new_file, config_parser.misc__verbose)
        go_parser = GeneOntologyParser(config_parser.go__url, config_parser.go__black_list_file_name, config_parser.misc__tmp_dir, config_parser.go__download_new_file, config_parser.misc__verbose)
        ppto_parser = PlantPhenotypeTraitOntologyParser(config_parser.ppto__url, config_parser.ppto__black_list_file_name, config_parser.misc__tmp_dir, config_parser.ppto__download_new_file, config_parser.misc__verbose)
        ontology_term_propagator = OntologyTermPropagator(to_parser, go_parser, ppto_parser, config_parser.misc__verbose)
        ncbi_to_plaza_linker = NcbiToPlazaLinker(db_handler, ontology_term_propagator, config_parser.plaza__url_dicots, config_parser.plaza__url_monocots,
                                                 config_parser.plaza__maize_mapping_url, config_parser.plaza__maize_mapping_v45_url, config_parser.plaza__wheat_mapping_v11_file_name,
                                                 config_parser.plaza__wheat_mapping_v21_file_name, config_parser.plaza__out_dir, config_parser.misc__tmp_dir, config_parser.misc__verbose)
        plaza_orthology_handler = OrthologyHandler(db_handler, config_parser.plaza__orthology_species_to_import, config_parser.export__max_ortho_links, config_parser.misc__verbose)


        #### DATABASE INITIALIZATION ####

        if config_parser.db__purge:
            db_handler.purge_database(purge_species_synonyms=config_parser.db__purge_species_synonyms,
                                      purge_gene_synonyms=config_parser.db__purge_gene_synonyms,
                                      purge_to=(config_parser.db__purge_trait_synonyms or config_parser.db__purge_bp_synonyms),
                                      purge_ncbi=config_parser.db__purge_ncbi_tables,
                                      purge_plaza_genes=config_parser.db__purge_plaza_synonyms,
                                      purge_plaza_orthology=config_parser.db__purge_plaza_orthology)

        if config_parser.species__import_to_db:  # should only be done once to save the time
            ncbi_taxonomy_parser = NcbiPlantTaxonomyParser(config_parser.species__url, config_parser.misc__tmp_dir)
            db_handler.import_species_dict(ncbi_taxonomy_parser.dictionary)

        if config_parser.genes__import_to_db:  # should only be done once to save the time
            ncbi_gene_parser = NcbiGeneIdentifierParser(config_parser.genes__url, config_parser.misc__tmp_dir)
            db_handler.import_gene_dict(ncbi_gene_parser.dictionary)

        if config_parser.to__import_to_db:  # should only be done once to save the time
            db_handler.import_ontology_dict(to_parser.dictionary, "TO")

        if config_parser.go__import_to_db:  # should only be done once to save the time
            db_handler.import_ontology_dict(go_parser.dictionary, "GO")

        if config_parser.ppto__import_to_db:  # should only be done once to save the time
            db_handler.import_ontology_dict(ppto_parser.dictionary, "PPTO")

        trait_syn_dict = to_parser.dictionary  # combined dictionary of all the trait synonyms
        trait_syn_dict.update(go_parser.dictionary)
        trait_syn_dict.update(ppto_parser.dictionary)


        #### PLAZA LINKING ####

        if config_parser.g2a__import_to_db:
            ncbi_to_plaza_linker.parse_and_import_gene2accession_file(config_parser.g2a__url)
        if config_parser.plaza__import_plaza_synonyms:
            ncbi_to_plaza_linker.import_plaza_synonyms()
        if config_parser.plaza__compute_links:
            ncbi_to_plaza_linker.compute_links(config_parser.plaza__blast_dir, config_parser.plaza__genome_links_file_name)

        if config_parser.plaza__import_orthology:
            plaza_orthology_handler.import_orthologies(config_parser.plaza__out_dir, config_parser.plaza__tree_based_orthology_url, config_parser.plaza__orthologous_gene_family_url, config_parser.plaza__bhi_family_url)


        #### TEXT MINING ####

        if config_parser.pubtator__import_to_db:
            # only annotation for species available in the database will be imported
            species_dict = db_handler.export_species_dict()
            pt_parser = PubTatorParser(db_handler, species_dict, trait_syn_dict, config_parser.pubtator__species_synonyms_black_list_file_name, config_parser.pubtator__gene_synonyms_black_list_file_name, config_parser.misc__out_dir, config_parser.misc__tmp_dir, verbose=False)
            pt_parser.parse_annotations(config_parser.pubtator__file_name_pattern, config_parser.pubtator__start_doc_idx, config_parser.pubtator__end_doc_idx)


        #### STATISTICS ####

        stats_extractor = StatsExtractor(db_handler, ontology_term_propagator, config_parser.stats__out_dir)

        if config_parser.stats__print_statistics:
            stats_extractor.print_statistics()

        if config_parser.stats__print_per_species_statistics:
            stats_extractor.print_per_species_statistics()

        if config_parser.stats__draw_all_plots or config_parser.stats__draw_max_score_vs_evidences:
            stats_extractor.draw_max_score_vs_evidences()

        if config_parser.stats__draw_all_plots or config_parser.stats__draw_bar_associations_per_case:
            stats_extractor.draw_bar_associations_per_case()

        if config_parser.stats__draw_all_plots or config_parser.stats__draw_bar_assoc_score_per_section_type:
            stats_extractor.draw_bar_assoc_score_per_section_type()

        if config_parser.stats__draw_all_plots or config_parser.stats__draw_bar_publications_per_year:
            stats_extractor.draw_bar_publications_per_year()

        if config_parser.stats__draw_all_plots or config_parser.stats__draw_bar_unique_triples_per_trait:
            stats_extractor.draw_bar_unique_triples_per_trait()

        if config_parser.stats__draw_all_plots or config_parser.stats__draw_hist_evidences_per_association:
            stats_extractor.draw_hist_evidences_per_association()

        if config_parser.stats__draw_all_plots or config_parser.stats__draw_hist_associations_per_paper:
            stats_extractor.draw_hist_associations_per_paper()

        if config_parser.stats__draw_all_plots or config_parser.stats__draw_hist_max_score_per_triple:
            stats_extractor.draw_hist_max_score_per_triple()

        if config_parser.stats__draw_all_plots or config_parser.stats__draw_hm_species_per_section:
            stats_extractor.draw_hm_species_per_section()

        if config_parser.stats__draw_all_plots or config_parser.stats__draw_hm_traits_per_species:
            stats_extractor.draw_hm_traits_per_species(20)

        if config_parser.stats__draw_all_plots or config_parser.stats__draw_hm_traits_per_species_selected:
            stats_extractor.draw_hm_traits_per_species_selected()

        if config_parser.stats__draw_all_plots or config_parser.stats__draw_hm_associations_per_paragraph_type:
            stats_extractor.draw_hm_associations_per_paragraph_type()

        if config_parser.stats__draw_all_plots or config_parser.stats__draw_upset_association_cases:
            stats_extractor.draw_upset_association_cases()


        #### DATA EXPORT ####

        if (config_parser.export__export_original_data or config_parser.export__export_orthology_data):

            data_exporter = DataExporter(db_handler, plaza_orthology_handler, ontology_term_propagator,
                                         config_parser.export__out_dir, trait_syn_dict, config_parser.misc__verbose)

            data_exporter.export_triples(config_parser.export__species_list_file_name,
                                            config_parser.export__export_original_data,
                                            config_parser.export__export_orthology_data, config_parser.export__only_high_quality,
                                            config_parser.export__tm_min_occurrence_threshold, config_parser.export__tm_min_occurrence_ortho_threshold,
                                            config_parser.export__tm_max_score_threshold, config_parser.export__tm_max_score_ortho_threshold,
                                            config_parser.export__max_ortho_links)


        #### MISCELLANEOUS ####

        if config_parser.misc__clear_tmp_files:
            for filename in os.listdir(config_parser.misc__tmp_dir):
                file_path = os.path.join(config_parser.misc__tmp_dir, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as exception:
                    print(f"Failed to delete {file_path}. Reason: {exception}.")

    except Exception as exception:
        tools.print_exception_message(f"Exception raized: {exception}", True)

    finally:
        tools.print_info_message(f"Pipeline ended on: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        end_time = time.time()
        tools.print_final_statistics(start_time, end_time)


if __name__ == '__main__':
    main()
