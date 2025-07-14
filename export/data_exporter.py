"""
Extracts the requested data from the database.
"""

import csv
import datetime
import os
import sys
from typing import Dict

import pandas as pd

from plaza.orthology_handler import OrthologyHandler
from tools import tools, constants
from tools.database_handler import DatabaseHandler
from tools.ontology_term_propagator import OntologyTermPropagator


class DataExporter():
    """
    Extracts data from the database and exports it in specified files.
    """

    def __init__(self, db_handler: DatabaseHandler, orthology_handler: OrthologyHandler, ontology_term_propagator: OntologyTermPropagator,
                 out_dir: str, trait_synonyms_dict: Dict, verbose: bool):
        """
        Constructor.

        Parameters
        ----------
        db_handler : DatabaseHandler
            object allowing to interact with the database
        orthology_handler : OrthologyHandler
            if specified, orthology information will be used for validation
        ontology_term_propatagor : OntologyTermPropagator
            allows to propagate parental terms
        only_high_quality : bool
            if yes: only high quality triples are exported, if no: high quality AND all triples are exported
        out_dir : str
            folder where the output should be stored
        trait_synonyms_dict : Dict
            dictionary linking trait ids to trait synonyms
        verbose : bool
            indicates whether more detailed messages should be printed
        """
        self.db_handler = db_handler
        self.orthology_handler = orthology_handler
        self.ontology_term_propagator = ontology_term_propagator

        self.out_dir = out_dir
        self.trait_synonyms_dict = trait_synonyms_dict
        self.verbose = verbose

        # triples-related, set default values here
        self.only_high_quality = False
        self.tm_min_occurrence_threshold = 0
        self.tm_min_occurrence_ortho_threshold = 0
        self.tm_max_score_threshold = 0
        self.tm_max_score_ortho_threshold = 80

        self.time_now = datetime.datetime.now().strftime("%Y-%m-%d--%H-%M-%S")
        self.output_file_prefix = f"export__{self.time_now}"
        self.output_file_suffix = ""

        self.writer = None
        self.workbook = None
        self.header_format = None
        self.long_text_format = None
        self.wrap_cell_format = None
        self.thousands_format = None
        self.decimals_format = None


    def export_triples(self, species_list_file_name: str,
                       export_original_data: bool, export_orthology_data: bool, only_high_quality: bool,
                       tm_min_occurrence_threshold: int, tm_min_occurrence_ortho_threshold: int,
                       tm_max_score_threshold: int, tm_max_score_ortho_threshold: int,
                       max_ortho_links: int):
        
        """
        Exports triples for the provided list of species.
        Two files are generated:
        1. Triples, in the GAF format
        2. Evidences for these triples, exported in a custom TSV format
        If a triple has evidences from multiple sources (TM),
        the first file will contain one line per source type (TM),
        each line will combine the references of that source type in the column 'db_reference'.
        The second file will contain one line per evidence for each triple in the first file.
        The triples in both files are identified by the fields ('species_id', 'trait_id', 'gene_name').
        Additionally, files for enrichment analysis and for MINI-EX are generated,
        both with and without parental term propagation.

        Parameters
        ----------
        species_list_file_name : str
            list of species for which triples have to be exported
        export_original_data : bool
            whether data collected for the species of interest has to be exported (vs orthology)
        export_orthology_data : bool
            whether data collected for other species and transferred via orthology to hte species of interest has to be exported
        only_high_quality : bool
            whether only high quality triples should be exported
        tm_min_occurrence_threshold : int
            if high quality: specifies threshold for the minimum number of TM evidences per triple
        tm_min_occurrence_ortho_threshold : int
            if high quality: specifies threshold for the minimum number of TM evidences per triple, for orthologous triples
        tm_max_score_threshold : int
            if high quality: specifies threshold for the minimum number of TM evidences per triple
        tm_max_score_ortho_threshold : int
            if high quality: specifies threshold for the minimum number of TM evidences per triple, for orthologous triples
        """
        tools.print_info_message("Exporting triples...", 0)

        ######## initialization ########
        self.export_original_data = export_original_data
        self.export_orthology_data = export_orthology_data

        self.only_high_quality = only_high_quality
        try:
            self.tm_min_occurrence_threshold = int(tm_min_occurrence_threshold)
        except ValueError:
            tools.print_exception_message(f"Invalid min occurrence threshold provided for high-quality text mining triples: '{tm_min_occurrence_threshold}'!")
            sys.exit(1)
        try:
            self.tm_min_occurrence_ortho_threshold = int(tm_min_occurrence_ortho_threshold)
        except ValueError:
            tools.print_exception_message(f"Invalid min occurrence threshold provided for high-quality text mining triples from orthology: '{tm_min_occurrence_ortho_threshold}'!")
            sys.exit(1)
        try:
            self.tm_max_score_threshold = int(tm_max_score_threshold)
        except ValueError:
            tools.print_exception_message(f"Invalid max score threshold provided for high-quality text mining triples: '{tm_max_score_threshold}'!")
            sys.exit(1)
        try:
            self.tm_max_score_ortho_threshold = int(tm_max_score_ortho_threshold)
        except ValueError:
            tools.print_exception_message(f"Invalid max score threshold provided for high-quality text mining triples from orthology: '{tm_max_score_ortho_threshold}'!")
            sys.exit(1)

        self._compute_output_file_suffix(max_ortho_links)

        ######## triple calculation ########
        items_per_line = 2
        triple_evidences_df = pd.DataFrame(columns = ['species_id', 'trait_id', 'gene_id', 'gene_name', 'gene_synonyms', 'evidence_code', 'db_reference', 'sctrait_annotations', 'ortho_species_id', 'ortho_gene_name'])

        with open(species_list_file_name, "r", encoding="utf-8") as species_list_file:
            for line in species_list_file:
                if not line.startswith('#'):  # this is a comment
                    line_parts = line.split('\t')
                    if len(line_parts) != items_per_line:
                        tools.print_warning_message(f"Incorrect number of items in the file {species_list_file_name}: {len(line_parts)} instead of {items_per_line}!")
                        continue
                    spec_name = line_parts[0]
                    tax_id = int(line_parts[1])

                    tools.print_info_message(f"Exporting triples for {spec_name}...")

                    if export_original_data:
                        tools.print_info_message(f"Collecting original information...", 2)
                        triple_evidences_df = pd.concat([triple_evidences_df, self._collect_triples_with_evidences(tax_id, False)])

                    if export_orthology_data:
                        tools.print_info_message("Collecting orthologous information...", 2)
                        triple_evidences_ortho_df = self.orthology_handler.get_orthologous_triples(tax_id, self._collect_triples_with_evidences)
                        triple_evidences_ortho_df['evidence_code'] = "ISO"
                        triple_evidences_df = pd.concat([triple_evidences_df, triple_evidences_ortho_df])

        ######## triple export ########
        triple_evidences_df.sort_values(['species_id', 'gene_name', 'trait_id'], ascending=[True, True, True], inplace=True)
        
        # export of the fwo files
        self._export_triples(triple_evidences_df)  # exports the triples in GAF format
        self._export_evidences(triple_evidences_df)  # exports the evidences for these triples in a custom format

        # additional files, for enrichment analysis and MINI-EX runs
        features_df = triple_evidences_df[['trait_id', 'gene_name', 'species_id']].drop_duplicates()

        self._export_triples_for_mini_ex(features_df, os.path.join(self.out_dir, f"{self.output_file_prefix}__mini-ex__{self.output_file_suffix}.tsv"))  # exports the triples as a GO file for MINI-EX

        # for enrichment analysis and MINI-EX the parental terms must be propagated
        if self.verbose:
            tools.print_info_message(f"Propagating parental terms for the {len(features_df):,} triples...", 3)
        features_df = self.ontology_term_propagator.propagate_parental_terms(features_df)
        if self.verbose:
            tools.print_info_message(f"Triples after propagation: {len(features_df):,}.", 4)

        self._export_triples_for_mini_ex(features_df, os.path.join(self.out_dir, f"{self.output_file_prefix}__mini-ex__{self.output_file_suffix}__prop.tsv"))  # exports the triples as a GO file for MINI-EX
                    
        tools.print_info_message("Done.")


    def _compute_output_file_suffix(self, max_ortho_links: int):
        """
        Computes suffix used for output files, depending on the parameters.
        """
        self.output_file_suffix = "tm"
        if self.export_original_data:
            self.output_file_suffix += "__orig"
            if self.only_high_quality:
                self.output_file_suffix += f"--oc-{self.tm_min_occurrence_threshold}"
                self.output_file_suffix += f"--ms-{self.tm_max_score_threshold}"
        if self.export_orthology_data:
            self.output_file_suffix += "__ortho"
            self.output_file_suffix += f"--1-to-{max_ortho_links}"
            if self.only_high_quality:
                self.output_file_suffix += f"--oc-{self.tm_min_occurrence_ortho_threshold}"
                self.output_file_suffix += f"--ms-{self.tm_max_score_ortho_threshold}"


    def _collect_triples_with_evidences(self, tax_id: int, is_orthology_search: bool):
        """
        Collects triples with evidences from all the sources for the species with the provided tax_id.

        Parameters
        ----------
        tax_id : int
            species tax id
        is_orthology_search : bool
            whether this is an orthology search (vs original data)

        Returns
        -------
        pd.DataFrame
            dataframe containing triples for the given species from all the sources
        """
        all_triples_df = pd.DataFrame(columns = ['species_id', 'trait_id', 'gene_id', 'gene_name', 'gene_synonyms', 'evidence_code', 'db_reference', 'sctrait_annotations'])
        all_triples_df = pd.concat([all_triples_df, self._collect_tm_triples_for_tax_id(tax_id, is_orthology_search)])
        
        return all_triples_df


    def _collect_tm_triples_for_tax_id(self, tax_id: int, is_orthology_search: bool):
        """
        Collects text mining triples for the provided species.
        If high quality filter is set: triples are filtered based on user-defined thresholds.

        Parameters
        ----------
        tax_id : int
            tax id of the species for which the triples have to be collected
        is_orthology_search : bool
            whether this is an orthology search (vs original data)

        Returns
        -------
        pd.DataFrame
            dataframe with text mining triples
        """
        tools.print_info_message(f"Collecting text mining information for tax id {tax_id}...", 3)
        tm_triples = self.db_handler.get_triples_with_tm_evidences_for_tax_id(tax_id)
        tm_triples_df = pd.DataFrame(tm_triples).transpose()
        if not tm_triples_df.empty:
            tm_triples_df.columns = ['species_id', 'trait_id', 'gene_id', 'gene_name', 'gene_synonyms', 'pubmed_id', 'max_score', 'ev_count']
            if self.verbose:
                tools.print_info_message(f"Collected {len(tm_triples_df[['trait_id', 'gene_id']].drop_duplicates()):,} TM triples.", 4)

            if self.only_high_quality:  # filter triples based on user-defined thresholds
                if is_orthology_search:
                    min_occurrence_threshold = self.tm_min_occurrence_ortho_threshold
                    max_score_threshold = self.tm_max_score_ortho_threshold
                else:
                    min_occurrence_threshold = self.tm_min_occurrence_threshold
                    max_score_threshold = self.tm_max_score_threshold

                grouped_df = tm_triples_df.groupby(['species_id', 'trait_id', 'gene_id']).agg({'ev_count': 'sum', 'max_score': 'max'}).reset_index()
                filtered_df = grouped_df[(grouped_df['ev_count'] >= min_occurrence_threshold) & (grouped_df['max_score'] >= max_score_threshold)].drop(columns=['ev_count', 'max_score'])
                tm_triples_df = pd.merge(filtered_df, tm_triples_df, on=['species_id', 'trait_id', 'gene_id'], how='inner')
                if self.verbose:
                    tools.print_info_message(f"After high quality filtering: {len(tm_triples_df[['trait_id', 'gene_id']].drop_duplicates()):,} triples left.", 4)

            if not tm_triples_df.empty:
                tm_triples_df['evidence_code'] = "TAS"
                tm_triples_df['db_reference'] = tm_triples_df.apply(lambda row : f"PMID:{row['pubmed_id']}", axis=1)
                tm_triples_df['sctrait_annotations'] = tm_triples_df.apply(lambda row : f"max_score:{row['max_score']}|ev_count:{row['ev_count']}", axis=1)
                tm_triples_df.drop(columns=['pubmed_id', 'max_score', 'ev_count'], inplace=True)
        else:
            tools.print_info_message("No TM triples found.", 4)
        return tm_triples_df
    

    def _export_triples(self, triple_evidences_df: pd.DataFrame):
        """
        Exports the provided triples in the GAF format.

        Parameters
        ----------
        triple_evidences_df : pd.DataFrame
            dataframe with triples to export
        """
        triples_df = self._join_db_reference(triple_evidences_df)
        date_now = datetime.datetime.now().strftime("%Y%m%d")
        gaf_df = pd.DataFrame(columns = (['db', 'db_object_id', 'db_object_symbol', 'qualifier', 'go_id', 'db_reference',
                                          'evidence_code', 'with_or_from', 'aspect', 'db_object_name', 'db_object_synonym',
                                          'db_object_type', 'taxon', 'date', 'assigned_by', 'annotation_extension', 'gene_product_form_id']))
        
        gaf_df['db_object_id'] = triples_df['gene_id']
        gaf_df['db_object_symbol'] = triples_df['gene_name']
        gaf_df['go_id'] = triples_df['trait_id']
        gaf_df['db_reference'] = triples_df['db_reference']
        gaf_df['evidence_code'] = triples_df['evidence_code']
        gaf_df['db_object_name'] = triples_df['gene_name']
        gaf_df['db_object_synonym'] = triples_df['gene_synonyms']
        gaf_df['taxon'] = triples_df['species_id']

        gaf_df['db'] = "scTrait"
        gaf_df['qualifier'] = "contributes_to"
        gaf_df['with_or_from'] = ""
        gaf_df['aspect'] = "P"
        gaf_df['db_object_type'] = "protein"
        gaf_df['date'] = date_now
        gaf_df['assigned_by'] = "scTrait"
        gaf_df['annotation_extension'] = ""
        gaf_df['gene_product_form_id'] = ""

        gaf_file_name = os.path.join(self.out_dir, f"{self.output_file_prefix}__triples__{self.output_file_suffix}.gaf")
        with open(gaf_file_name, "w", encoding="utf-8") as output_file:
            if self.verbose:
                tools.print_info_message(f"Exporting triples to {gaf_file_name}...", 2)
            output_file.write("!gaf-version: 2.1\n")
            gaf_df.to_csv(output_file, header=False, index=False, sep='\t')


    def _join_db_reference(self, triple_evidences_df: pd.DataFrame):
        """
        Removes duplicated triples by joining their 'db_reference' fields of the same reference type.
        For example, if a triple for species S, gene G and trait T
        has following evidences:
        S G T PMID 1
        S G T PMID 2
        S G T PMID 3
        three following lines will be created:
        S G T PMID 1|PMID 2|PMID 3

        Parameters
        ----------
        triple_evidences_df : pd.DataFrame
            DataFrame with the full list of triples and evidences

        Returns
        -------
        pd.DataFrame
            triples with joined 'db_reference' fields
        """
        tm_triples_df = triple_evidences_df[triple_evidences_df['db_reference'].str.startswith('PMID:')]
        
        all_triples_df = tm_triples_df.groupby(['species_id', 'trait_id', 'gene_id', 'gene_name', 'gene_synonyms', 'evidence_code'], as_index=False).agg({'db_reference': '|'.join})

        return all_triples_df


    def _export_evidences(self, triple_evidences_df: pd.DataFrame):
        """
        Exports the provided dataframe with evidences for triples in a custom format.

        Parameters
        ----------
        triple_evidences_df : pd.DataFrame
            dataframe with evidences to export
        """
        evidences_df = triple_evidences_df[['gene_name', 'trait_id', 'species_id', 'evidence_code', 'db_reference', 'sctrait_annotations', 'ortho_species_id', 'ortho_gene_name']].drop_duplicates()
        evidences_file_name = os.path.join(self.out_dir, f"{self.output_file_prefix}__evidences__{self.output_file_suffix}.tsv")
        with open(evidences_file_name, "w", encoding="utf-8") as output_file:
            if self.verbose:
                tools.print_info_message(f"Exporting evidences to {evidences_file_name}...", 2)
            evidences_df.to_csv(output_file, header=False, index=False, sep='\t')


    def _export_triples_for_enrichment(self, features_df: pd.DataFrame, out_file_name: str):
        """
        Exports the provided dataframe as a feature file to be used for enrichment analysis,
        with three columns: 'gene_name', 'trait_id' and 'trait_description'.

        Parameters
        ----------
        features_df : pd.DataFrame
            dataframe to export, with the following columns: ['trait_id', 'gene_name', 'species_id']
        out_file_name : str
            output file name
        """
        features_with_trait_synonyms_df = features_df.copy()
        features_with_trait_synonyms_df['trait_id_synonyms'] = features_with_trait_synonyms_df['trait_id'].map(self.trait_synonyms_dict)  # extend trait id with its synonyms
        features_with_trait_synonyms_df = features_with_trait_synonyms_df.dropna()  # some parental terms are "molecular function" and became "nan" after propagation -> we remove them
        features_with_trait_synonyms_df['trait_description'] = features_with_trait_synonyms_df['trait_id_synonyms'].apply(self._extract_first_synonym_from_list)
        features_with_trait_synonyms_df = features_with_trait_synonyms_df[features_with_trait_synonyms_df['trait_description'] != '']
        result = features_with_trait_synonyms_df[['gene_name', 'trait_id', 'trait_description']]

        if self.verbose:
            self._print_features_statistics(features_df)
            tools.print_info_message(f"Exporting features to {out_file_name}...", 2)
        result.to_csv(out_file_name, header=False, index=False, sep='\t')


    def _extract_first_synonym_from_list(self, synonyms: str) -> str:
        """
        Extracts the first synonym from a list of synonyms, separated by '|'.

        Parameters
        ----------
        synonyms : str
            list of synonyms of a term

        Returns
        -------
        str
            first synonym
        """
        parts = synonyms.split('|')
        if len(parts) > 1:
            return parts[1].strip()
        else:
            return ''


    def _export_triples_for_mini_ex(self, features_df: pd.DataFrame, out_file_name: str):
        """
        Exports the provided dataframe as a GO file to be used with MINI-EX, having following columns:
        - trait id
        - gene name
        - evidence code (replaced by 'SCTRAIT')
        - trait synonyms

        Parameters
        ----------
        features_df : pd.DataFrame
            dataframe to expert, with the following columns: ['trait_id', 'gene_name', 'species_id']
        out_file_name : str
            output file name
        """
        result_df = pd.DataFrame()
        result_df['trait_id'] = features_df['trait_id']
        result_df['gene_name'] = features_df['gene_name']
        result_df['evidence_code'] = "SCTRAIT"
        result_df['trait_synonyms'] = features_df['trait_id'].map(self.trait_synonyms_dict)  # extend trait id with its synonyms
        result_df = result_df.dropna()  # some parental terms are "molecular function" -> we remove them

        if self.verbose:
            tools.print_info_message(f"Exporting features in MINI-EX format to {out_file_name}...", 2)
        result_df.to_csv(out_file_name, sep='\t', index=False, header=False)


    def _print_features_statistics(self, features_df: pd.DataFrame):
        """
        Prinsts statistics about the collected triples.

        Parameters
        ----------
        features_df : pd.DataFrame
            dataframe with the collected triples
        """
        tools.print_info_message(f"Collected {len(features_df):,} triples:", 2)
        tools.print_info_message(f"{features_df['trait_id'].nunique():,} unique traits", 3)
        tools.print_info_message(f"{features_df['gene_name'].nunique():,} unique genes", 3)
        tools.print_info_message(f"Median genes per term: {features_df.groupby('trait_id')['gene_name'].nunique().median():,}", 3)


    def _init_excel_writer(self, file_name: str):
        """
        Initializes Excel-related features.

        Parameters
        ----------
        file_name : str
            name of the Excel file
        """
        self.writer = pd.ExcelWriter(file_name, engine='xlsxwriter')  # pylint: disable=import-error,abstract-class-instantiated
        self.workbook = self.writer.book  # pylint: disable=no-member
        self.header_format = self.workbook.add_format({'bold': True, 'text_wrap': True, 'align': 'center', 'fg_color': '#D7E4BC', 'border': 1})
        self.long_text_format = self.workbook.add_format({'bold': False, 'align': 'left', 'border': 0})
        self.thousands_format = self.workbook.add_format({'num_format': '#,##0'})
        self.decimals_format = self.workbook.add_format({'num_format': '#.##0'})
        self.wrap_cell_format = self.workbook.add_format({'text_wrap': True})
