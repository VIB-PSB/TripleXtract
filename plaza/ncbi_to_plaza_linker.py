"""
Creates links between PLAZA and NCBI gene identifiers, either by means of their names,
or from transcript sequence similarity.
"""


import gzip
import subprocess                   # to run programs in command line
import urllib.request               # to download files
from typing import List, Set
import os

import requests
import pandas as pd
from clint.textui import progress

from export.stats_extractor import StatsExtractor
from tools import tools, constants
from tools.database_handler import DatabaseHandler
from tools.exceptions import FileFormatError
from tools.html_parser import CustomHtmlParser
from tools.ontology_term_propagator import OntologyTermPropagator



class NcbiToPlazaLinker():
    """
    Creates links between NCBI and PLAZA gene identifiers using sequence comparison.
    """

    def __init__(self, db_handler: DatabaseHandler, ontology_term_propagator: OntologyTermPropagator, plaza_url_dicots: str, plaza_url_monocots: str, maize_mapping_v345_url: str,
                maize_mapping_v45_url: str, wheat_mapping_v11_file_name: str, wheat_mapping_v21_file_name: str, out_dir: str, tmp_dir: str, verbose: bool = True):
        """
        Constructor.
        Initializes internal values and downloads PLAZA files with gene identifiers.

        Parameters
        ----------
        db_handler : DatabaseHandler
            database handler, used to perform database requests
        ontology_term_propagator : OntologyTermPropagator
            allows to propagate ontology parental terms
        plaza_url_dicots : str
            PLAZA URL that contains the list of PLAZA files with gene identifiers for dicots
        plaza_url_monocots : str
            PLAZA URL that contains the list of PLAZA files with gene identifiers for monocots
        maize_mapping_v345_url : str
            URL with maize gene mapping for v3, v4 and v5
        maize_mapping_v45_url : str
            URL with maize gene mapping for v4 and v5 (only for genes missing in v3)
        wheat_mapping_v11_file_name : str
            file name with wheat gene mappings for v1 (having 01G in the id) to v1.1 (having 02G in the id) (the file is only available in a ZIP archive, so no direct link)
        out_dir : str
            directory for output files
        tmp_dir : str
            directory for temporary files
        verbose : bool, optional
            indicates when log messages should be printed; bu default True
        """
        self.db_handler = db_handler
        self.maize_mapping_v345_df = self._parse_genome_version_file(maize_mapping_v345_url, ['v3_id', 'v4_id', 'v5_id'])
        self.maize_mapping_v45_df = self._parse_genome_version_file(maize_mapping_v45_url, ['v4_id', 'v5_id'])
        self.wheat_mapping_v11_df = self._parse_genome_version_file(wheat_mapping_v11_file_name, ['v1_id', 'v11_id', '-', 'status'])
        self.wheat_mapping_v21_df = self._parse_genome_version_file(wheat_mapping_v21_file_name, ['v11_id', 'v21_id'])
        self.out_dir = out_dir
        self.tmp_dir = tmp_dir
        self.wheat_mapping_df = None
        self.plaza_tax_ids = []
        self._plaza_file_names = []
        self.plaza_url_dicots = plaza_url_dicots
        self.plaza_url_monocots = plaza_url_monocots
        self.verbose = verbose
        self.stats_extractor = StatsExtractor(self.db_handler, ontology_term_propagator, self.out_dir)
        
        
    def get_plaza_file_names(self):
        """
        Provides the list of the PLAZA file names, for dicots and monocots.
        If the files have not been downloaded yet, downloads them and returns the updated list.

        Returns
        -------
        List
            list of the PLAZA file names
        """
        if len(self._plaza_file_names) == 0:
            self._download_plaza_id_files(self.plaza_url_dicots)
            self._download_plaza_id_files(self.plaza_url_monocots)
        return self._plaza_file_names
    
    
    plaza_file_names = property(get_plaza_file_names)
        
        
    def _parse_genome_version_file(self, file_url: str, column_names: List):
        """
        Stores a content of the provided file URL to a dataframe.
        This dataframe will allow to convert gene names from different genome versions.
        
        Parameters
        ----------
        file_url : str
            URL of the file containing gene conversions
        column_names : List
            names of the columns in the provided file

        Returns
        -------
        _type_
            _description_
        """
        version_df = pd.read_csv(file_url, sep='\t')
        version_df.columns = column_names
        version_df.dropna(axis=0, inplace=True)
        version_df.reset_index(inplace=True)
        return version_df


    def extract_plaza_gene_id_for_maize(self, gene_id: str):
        """
        Allows to convert maize gene identifiers from previous versions to the current maize version (v5).

        Parameters
        ----------
        gene_id : str
            name of the gene

        Returns
        -------
        str
            PLAZA gene identifier
        """
        plaza_gene_id = -1
        matches = self.maize_mapping_v345_df.index[(self.maize_mapping_v345_df['v3_id'] == gene_id) | (self.maize_mapping_v345_df['v4_id'] == gene_id) | (self.maize_mapping_v345_df['v5_id'] == gene_id) ].tolist()
        if len(matches) == 1:  # if there is more than one mapping, we discard it to avoid creating false positives
            plaza_gene_id = self.maize_mapping_v345_df.iloc[matches[0]]['v5_id']
        if plaza_gene_id == -1:
            matches = self.maize_mapping_v45_df.index[(self.maize_mapping_v45_df['v4_id'] == gene_id) | (self.maize_mapping_v45_df['v5_id'] == gene_id) ].tolist()
            if len(matches) == 1:
                plaza_gene_id = self.maize_mapping_v45_df.iloc[matches[0]]['v5_id']
        return plaza_gene_id


    def convert_wheat_gene_name(self, gene_id: str):
        """
        Allows to convert wheat gene identifiers from v1.0 to v2.1.
        Two files are provided: v1.1 and v2.1 conversion.
        From the v1.1 file, all genes marked as unchanged are kept and the names of v1.2 genes are updated from "01G" to "O2G"
        (dont' know why it is not the case in the original file).
        Then v1.1 and v2.1 files are merged, to provide conversion from v1.0 to v2.1.

        Parameters
        ----------
        gene_id : str
            name of the gene
        maize_versions_df : pd.DataFrame
            DataFrame with v3-v4-v5 gene identifiers conversions
        maize_versions_45_df : pd.DataFrame
            DataFrame with v4-v5 gene identifiers conversions, for genes identifiers missing in v3

        Returns
        -------
        str
            PLAZA gene identifier
        """
        if self.wheat_mapping_df is None:  # the dataframe has not yet been processed
            self.wheat_mapping_v11_df = self.wheat_mapping_v11_df.loc[self.wheat_mapping_v11_df['status'] == "no_changes"]  # only keep unchanged gene names
            self.wheat_mapping_v11_df['v11_id'] = self.wheat_mapping_v11_df['v11_id'].str.replace('01G', '02G')
            self.wheat_mapping_v11_df.pop('status')  # remove useless columns
            self.wheat_mapping_v11_df.pop('-')
            self.wheat_mapping_df = pd.merge(self.wheat_mapping_v11_df, self.wheat_mapping_v21_df, on='v11_id', how="inner")
            self.wheat_mapping_df.pop('index_x')
            self.wheat_mapping_df.pop('index_y')
            self.wheat_mapping_df.reset_index(inplace=True)
        plaza_gene_id = -1
        matches = self.wheat_mapping_df.index[(self.wheat_mapping_df['v1_id'] == gene_id)].tolist()
        if len(matches) == 1:  # if there is more than one mapping, we discard it to avoid creating false positives
            plaza_gene_id = self.wheat_mapping_df.iloc[matches[0]]['v21_id']
        return plaza_gene_id
    
    
    def import_plaza_synonyms(self):
        """
        Parses PLAZA files (each file provides gene identifiers of one species).
        For each parsed file, calls a private function that imports PLAZA synonyms.

        Parameters
        ----------
        func_to_call : function, optional
            the function to call for each parsed file, by default None
        """
        tools.print_info_message("IMPORTING PLAZA SYNONYMS...", 0)
        skip_idx = 0 # 9 for ath, 63 for osa
        self.plaza_tax_ids = [None] * skip_idx
        try:
            for plaza_file_name in self.plaza_file_names[skip_idx:]:
                with gzip.open(plaza_file_name, 'rb') as plaza_file:
                    next(plaza_file)  # PLAZA instance
                    next(plaza_file)  # File generation timestamp
                    next(plaza_file)  # Species information
                    plaza_spec_name = next(plaza_file).decode("utf-8").split(' : ')[-1].strip()  # - species : XXXX
                    common_name = next(plaza_file).decode("utf-8").split(' : ')[-1].strip()  # - common name : XXXX xxxx
                    tax_id = int(next(plaza_file).decode("utf-8").split(' : ')[-1].strip())  # - tax id : XXXX
                    next(plaza_file)  # - assembly/annotation source/version
                    next(plaza_file)  # - annotation data provider
                    next(plaza_file)  # gene_id    id_type    id

                    self.db_handler.add_plaza_spec_id(tax_id, plaza_spec_name)

                    self.plaza_tax_ids.append(tax_id)

                    self._import_plaza_synonyms(plaza_file, tax_id, common_name)
        except Exception as exception:
            tools.print_exception_message(f"Error has occurred during PLAZA identifiers analysis: {exception}")

        
    def _import_plaza_synonyms(self, plaza_file: gzip.GzipFile, tax_id: int, common_name: str):
        """
        Imports gene synonyms used by PLAZA to the database.

        Parameters
        ----------
        plaza_file : gzip.GzipFile
            file with PLAZA gene identifiers
        tax_id : int
            tax id of the species
        common_name : str
            common name of the species
        """
        tools.print_info_message(f"Importing synonyms for {common_name}...")
        synonyms = []
        all_synonyms = set()
        for line in plaza_file:
            line_parts = line.decode("utf-8").split('\t')  # a line is in form "plaza_id    type    identifier_value"
            plaza_id = line_parts[0].strip()
            id_value = line_parts[2].strip()
            synonyms.append((tax_id, plaza_id, id_value))
            if tax_id == 39947:  # a patch for O. sativa: we want to extract gene names from transcript names
                all_synonyms.add(id_value)
                if id_value.startswith("LOC_"):
                    value_parts = id_value.split('.')  # the format is LOC_Os01g01050.1, we want to remove the transcript part
                    gene_name = value_parts[0]
                    if gene_name not in all_synonyms:
                        all_synonyms.add(gene_name)
                        synonyms.append((tax_id, plaza_id, gene_name))
        self.db_handler.import_plaza_synonyms(synonyms)


    def parse_and_import_gene2accession_file(self, gene2accession_file_url: str):
        """
        Parses the NCBI file "gene2accession", providing accession numbers for every NCBI gene.
        Imports the relevant information to the database.
        Each entry of the file provides the following information:
        0 	tax_id
        1 	gene_id
        2 	status
        3 	rna_nucl_acc_version
        4 	rna_nucl_gi
        5 	protein_acc_version
        6 	protein_gi
        7 	gen_nucl_acc_version
        8 	gen_nucl_gi
        9 	start_pos_gen_acc
        10 	end_pos_gen_acc
        11 	orientation
        12 	assembly
        13 	mat_pept_acc_version
        14 	mat_pept_gi
        15 	symbol

        Parameters
        ----------
        gene2accession_file_url : str
            URL of the gene2accession file
        """
        tools.print_info_message("IMPORTING GENE2ACCESSION FILE...", 0)  # takes 2h30 on midas
        gene2accession_file_name = os.path.join(self.tmp_dir, "gene2accession.tsv")
        tools.download_and_extract_gz_file(gene2accession_file_url, gene2accession_file_name, True)
        with open(gene2accession_file_name, encoding="utf-8") as gene2accession_file:
            next(gene2accession_file)
            values = []
            cnt = 0
            for line in gene2accession_file:
                line_parts = line.split('\t')
                cnt += 1
                if len(line_parts) != 16:
                    tools.print_warning_message(f"Invalid number of arguments on line {cnt}: {line}")
                    continue
                tax_id = line_parts[0]
                gene_id = line_parts[1]
                rna_nucl_acc_version = line_parts[3]
                protein_acc_version = line_parts[5]
                gen_nucl_acc_version = line_parts[7]
                symbol = line_parts[15]
                values.append([tax_id, gene_id, rna_nucl_acc_version, protein_acc_version, gen_nucl_acc_version, symbol])
                if cnt%100 == 0:
                    self.db_handler.add_gene2accession_info(values)
                    values.clear()
                if self.verbose and cnt%10_000_000 == 0:
                    tools.print_info_message(f"{cnt:,} entries imported so far...")
        tools.print_info_message(f"Imported {cnt:,} entries.")
        

    def compute_links(self, blast_dir: str, genome_links_file_name: str):
        """
        Parses the file with the links to NCBI and PLAZA transcripts for each species, a tab-separated file in following format:
        'spec name' TAB 'tax id' TAB 'NCBI transcripts URL' TAB 'PLAZA version link' TAB 'PLAZA spec abbr' TAB 'percentage identity'
        For each line, downloads the NCBI transcripts file, PLAZA transcripts file and PLAZA isoform file (used to match transcript names).
        Performs BLAST alignment between the NCBI and PLAZA transcritps, only stores the top hit with identity greater than the specified percentage identity.
        Merges these matches with the NCBI gene information to retrieve PLAZA-NCBI links.
        PLAZA genes that couldn't be linked to NCBI identifiers are then imported separately.

        Parameters
        ----------
        blast_dir : str
            directory to blast executables
        genome_links_file_name : str
            name of the file with links to NCBI and PLAZA genomes

        Raises
        ------
        FileFormatError
            raised if a line of the parsed file is in wrong format (does not have exactly 5 elements)
        """
        tools.print_info_message("COMPUTING NCBI/PLAZA LINKS...", 0)
        tools.print_info_message("Removing previous links...")
        self.db_handler.remove_ncbi_plaza_links()
        with open(genome_links_file_name, encoding="utf-8") as genome_links_file:
            for line in genome_links_file:
                try:
                    if line.startswith("#"):  # this is a comment
                        continue
                    line = line.strip().split('\t')
                    entries_cnt = 7
                    if len(line) != entries_cnt:
                        raise FileFormatError(f"Incorrect format in the file {genome_links_file_name}: {len(line)} entries found instead of {entries_cnt}.")
                    common_name = line[0]
                    spec_id = int(line[1])
                    plaza_spec_name = line[2]
                    ncbi_url = line[3]
                    plaza_transcripts_url = line[4]
                    plaza_isoform_url = line[5]
                    perc_identity = int(line[6])

                    tools.print_info_message(f"Handling {common_name}...")
                    if spec_id == constants.TAX_ID__ARABIDOPSIS_THALIANA:  # A. thaliana: links are computed using gene identifiers
                        matched_genes = self._compute_links_for_a_thaliana()
                        self._import_unlinked_plaza_genes(plaza_spec_name, matched_genes)
                    else:
                        ncbi_file_name = self._download_file(ncbi_url, "ncbi", common_name)
                        plaza_transcripts_file_name = self._download_file(plaza_transcripts_url, "plaza", common_name)
                        plaza_isoform_file_name = self._download_file(plaza_isoform_url, "plaza", common_name)

                        g2a_df = self._get_gene2accession_df(spec_id)
                        if g2a_df is not None:
                            blast_df = self._get_blast_matches_df(blast_dir, common_name, plaza_transcripts_file_name, ncbi_file_name, perc_identity)
                            merged_df = self._merge_blast_matches_and_ncbi_gene_information(g2a_df, blast_df, plaza_isoform_file_name)
                            matched_genes = self._update_plaza_identifiers_in_db(merged_df)
                            self._import_unlinked_plaza_genes(plaza_spec_name, matched_genes[1])
                except FileFormatError as exception:
                    tools.print_exception_message(exception)
        tools.print_info_message("Done.", 2)
        
        
    def _compute_links_for_a_thaliana(self):
        """
        For A. thaliana, NCBI/PLAZA links can be easily computed using gene names:
        NCBI's locus tag corresponds to PLAZA's gene identifier.

        Returns
        -------
        Set
            list of PLAZA gene identifiers that could be linked
        """
        tools.print_info_message("Computing links from gene identifiers...", 2)
        gene_info = self.db_handler.get_gene_info_for_tax_id(constants.TAX_ID__ARABIDOPSIS_THALIANA)
        gene_info_df = pd.DataFrame(gene_info).transpose()
        linked_plaza_identifiers = set()
        if not gene_info_df.empty:
            gene_info_df.columns = ['id', 'ncbi_id', 'ncbi_synonyms', 'symbol', 'locus_tag', 'db_xref']
            plaza_info = set(self.db_handler.get_plaza_gene_ids_for_tax_id(constants.TAX_ID__ARABIDOPSIS_THALIANA))
            ncbi_plaza_links = []
            cnt = 0
            gene_info_df.apply(self._compute_links_for_a_thaliana_row, args=(plaza_info, ncbi_plaza_links, linked_plaza_identifiers, cnt), axis=1)
            self.db_handler.add_plaza_synonyms(ncbi_plaza_links)
            tools.print_info_message(f"Linked {len(ncbi_plaza_links):,} out of {len(gene_info_df):,} NCBI genes.", 3)
        else:
            tools.print_warning_message("The geneinfo table is empty, cannot compute links for A. thaliana.")
        return linked_plaza_identifiers


    def _compute_links_for_a_thaliana_row(self, row: pd.Series, plaza_info: Set, ncbi_plaza_links: List, linked_plaza_identifiers: Set, cnt: int):
        """
        For A. thaliana, a link between a NCBI gene and a PLAZA gene is created when the locus tag of a NCBI gene
        corresponds to PLAZA's gene identifier.

        Parameters
        ----------
        row : pd.Series
            a row from the NCBI's gene_info table
        plaza_info : Set
            set of PLAZA gene identifiers for A. thaliana
        ncbi_plaza_links : List
            resulting list of NCBI/PLAZA links
        linked_plaza_identifiers : Set
            set of linked PLAZA gene identifiers
        cnt : Int
            number of processed entries
        """
        cnt += 1
        if row['locus_tag'] in plaza_info:
            ncbi_plaza_links.append((row['ncbi_id'], row['locus_tag']))
            linked_plaza_identifiers.add(row['locus_tag'])
        if self.verbose and cnt % 10_000 == 0:
            tools.print_info_message(f"{cnt:,} gene_info entries processed so far...")



    def _download_plaza_id_files(self, url: str):
        """
        Parses the provided URL to retrieve the list of PLAZA files with gene identifiers.
        Each file is then downloaded and its local file name is stored.

        Parameters
        ----------
        url : str
            the URL to parse

        Returns
        -------
        List[str]
            list of file names of downloaded files
        """
        tools.print_info_message(f"Retrieving PLAZA id files to download from {url}...", 2)
        req = requests.get(url, 'html.parser', timeout=10)
        html_parser = CustomHtmlParser("id_conversion")
        html_parser.feed(req.text)
        tools.print_info_message(f"Retrieved {len(html_parser.file_links)} files.", 3)
        
        tools.print_info_message("Downloading PLAZA id files...", 2)
        
        for file_url in progress.bar(html_parser.file_links):
            file_url = file_url[2:]  # remove the './' part from the URL
            matching_file_names = [file_name for file_name in self._plaza_file_names if file_url in file_name]
            if len(matching_file_names) == 0:  # some species are present in plaza dicots and monocots: check that this file has not been downloaded yet
                plaza_file_name = os.path.join(self.tmp_dir, file_url)
                urllib.request.urlretrieve(url + "/" + file_url, plaza_file_name)
                self._plaza_file_names.append(plaza_file_name)


    def _download_file(self, url: str, prefix: str, common_name: str):
        """
        Downloads the files from the provided url and stores it with a name combined from the provided suffis and species common name.

        Parameters
        ----------
        url : str
            url to download
        prefix : str
            prefix to use for the file name
        common_name : str
            species common name to use for the file name

        Returns
        -------
        str
           the name of the downloaded file
        """
        if self.verbose:
            tools.print_info_message(f"Downloading file {url}...", 2)
        file_name = os.path.join(self.tmp_dir, f"{prefix}__{common_name.replace(' ', '_')}__{url.split('/')[-1]}")
        tools.download_url(url, file_name)
        return file_name


    def _merge_blast_matches_and_ncbi_gene_information(self, g2a_df: pd.DataFrame, blast_df: pd.DataFrame, plaza_isoform_file_name: str):
        """
        Merges the gene2accession dataframe with BLAST matches dataframe, on the column 'protein_acc_version',
        in order to create links between NCBI gene ids and PLAZA transcript names.
        After that, merges the obtained dataframe with PLAZA isoform dataframe, on the column 'plaza_isoform',
        in order to create links between PLAZA transcript names and PLAZA gene names.

        Parameters
        ----------
        g2a_df : pd.DataFrame
            dataframe corresponding to the NCBI gene2accession file
        blast_df : pd.DataFrame
            dataframe with results of BLAST alignments between NCBI and PLAZA transcripts
        plaza_isoform_file_name : str
            file name of plaza isoform file

        Returns
        -------
        _type_
            _description_
        """
        tools.print_info_message("Merging BLAST matches and NCBI gene2accession information...", 2)
        merged_df = pd.merge(g2a_df, blast_df, how="inner", on=["protein_acc_version"])
        tools.print_info_message(f"Found {len(merged_df):,} links from {len(g2a_df):,} gene2accession items and {len(blast_df):,} alignments.", 3)

        isoform_df = pd.read_csv(plaza_isoform_file_name, sep='\t', skiprows=2)
        isoform_df.columns = ['plaza_isoform', 'plaza_gene_id']
        merged_df = pd.merge(merged_df, isoform_df, how="inner", on=["plaza_isoform"])
        tools.print_info_message(f"Retrieved {len(merged_df):,} PLAZA gene names from isoforms.", 2)
        return merged_df


    def _update_plaza_identifiers_in_db(self, merged_df: pd.DataFrame):
        """
        Adds PLAZA gene ids to the corresponding NCBI gene ids from the provided dataframe.
        The dataframe is first filtered to remove duplicates (resulting from multiple isoform alignments).
        If after that removal one NCBI gene is linked to different PLAZA genes or one PLAZA gene is linked
        to different NCBI genes, these duplicates are removed.

        Parameters
        ----------
        merged_df : pd.DataFrame
            dataframe containing links between PLAZA and NCBI gene ids

        Returns
        -------
        List
            list of linked genes, composed of one list of unique NCBI identifiers and one list of unique PLAZA identifiers
        """
        merged_df.drop(columns=['protein_acc_version', 'plaza_isoform'], inplace=True)
        init_len = len(merged_df)
        merged_df = merged_df.sort_values('bitscore', ascending=False).drop_duplicates(subset=['plaza_gene_id', 'ncbi_gene_id'])
        tools.print_info_message(f"Removed {init_len-len(merged_df):,} duplicated matches.", 2)

        init_len = len(merged_df)
        merged_df = merged_df.drop_duplicates('ncbi_gene_id')
        tools.print_info_message(f"Removed {init_len - len(merged_df):,} NCBI genes with matches to distinct PLAZA genes.", 2)

        init_len = len(merged_df)
        merged_df = merged_df.drop_duplicates('plaza_gene_id')
        tools.print_info_message(f"Removed {init_len - len(merged_df):,} PLAZA genes with matches to distinct NCBI genes.", 2)

        tools.print_info_message(f"Updating {len(merged_df):,} PLAZA-NCBI links in the database...", 2)
        result = tuple(merged_df[['ncbi_gene_id', 'plaza_gene_id']].to_records(index=False))
        self.db_handler.add_plaza_synonyms(result)

        return [set(merged_df['ncbi_gene_id']), set(merged_df['plaza_gene_id'])]


    def _get_gene2accession_df(self, tax_id: int):
        """
        Provides a DataFrame with data from the NCBI gene2accession file for the provided species tax id.

        Parameters
        ----------
        tax_id : int
            tax id of the species of interest

        Returns
        -------
        pd.DataFrame
            resulting DataFrame
        """
        tools.print_info_message("Retrieving NCBI gene2accession information...", 2)
        g2a_df = None
        g2a_data = self.db_handler.get_gene2accession_info_for_tax_id(tax_id)
        if g2a_data:
            tools.print_info_message(f"Retrieved {len(g2a_data[1]):,} entries and {len(set(g2a_data[1])):,} unique genes.", 3)
            g2a_df = pd.DataFrame(g2a_data).transpose()
            g2a_df.columns = ['tax_id', 'ncbi_gene_id', 'rna_nucl_acc_version', 'protein_acc_version', 'gen_nucl_acc_version', 'symbol']
            g2a_df.drop(['tax_id', 'rna_nucl_acc_version', 'gen_nucl_acc_version', 'symbol'], axis=1, inplace=True)
        else:
            tools.print_warning_message("No entries were found. Sequence similarity cannot be computed.")
        return g2a_df


    def _get_blast_matches_df(self, blast_dir: str, common_name: str, plaza_transcripts_file_name: str, ncbi_file_name: str, perc_identity: int):
        """
        Performs a BLASTn alignment between PLAZA and NCBI transcripts and then filters results by only keeping matches
        with more than 99% identity.

        Parameters
        ----------
        blast_dir : str
            directory name of BLAST executables
        common_name : str
            species common name
        plaza_transcripts_file_name : str
            name of the file with PLAZA transcripts
        ncbi_file_name : str
            name of the file with NCBI transcripts
        perc_identity : int
            percentage of identity for BLAST hits

        Returns
        -------
        pd.DataFrame
            DataFrame with filtered BLASTN results
        """
        blast_alignment_file = self._perform_blast_alignment(blast_dir + "/blastn", common_name.lower().replace(' ', '_'), plaza_transcripts_file_name, ncbi_file_name)
               
        tools.print_info_message(f"Retrieving matches with >{perc_identity}% identity...", 2)
        blast_df = pd.read_csv(blast_alignment_file, sep='\t')
        blast_df.columns = ['qseqid', 'sseqid', 'pident', 'length', 'mismatch', 'gapopen', 'qstart', 'qend', 'sstart', 'send', 'evalue', 'bitscore']
        blast_df.drop(['length', 'mismatch', 'gapopen', 'qstart', 'qend', 'sstart', 'send'], axis=1, inplace=True)
        blast_df = blast_df.sort_values('bitscore', ascending=False).drop_duplicates('qseqid').sort_index()
        
        self.stats_extractor.draw_histogram(blast_df['pident'], 100)
        self.stats_extractor.set_plot_properties(common_name, "score", "number of hits", filename=f"Histogram - {common_name}", y_scale_is_log=True)
        
        init_matches = len(blast_df)
        blast_df = blast_df.loc[blast_df['pident'] > perc_identity]
        filtered_matches = len(blast_df)
        tools.print_info_message(f"Removed {init_matches - filtered_matches:,} from {init_matches:,} matches, {filtered_matches:,} matches remaining.", 3)

        blast_df.rename({'qseqid' : 'protein_acc_version', 'sseqid' : 'plaza_isoform'}, axis=1, inplace=True)  # needed to perform a merge on these columns
        blast_df['protein_acc_version'] = blast_df['protein_acc_version'].apply(self._extract_protein_acc_version_from_fasta_header)
        return blast_df        


    def _extract_protein_acc_version_from_fasta_header(self, fasta_header: str):
        # protein_acc_version is in the form "lcl|NC_003070.9_cds_NP_001318899.1_2 [gene=ARV1] [locus_tag=AT1G01020] ...", need to extract "NP_001318899.1"
        result = fasta_header[(fasta_header.find('cds_') + 4):]  # NP_001318899.1_2 [gene=ARV1] [locus_tag=AT1G01020] ..."
        result = result[:result.find('_', result.find('_') + 1)]  # NP_001318899.1
        return result


    def _perform_blast_alignment(self, blast_binary: str, ref_name: str, plaza_file_name: str, ncbi_file_name: str):
        """
        Performs a BLASTN alignment between PLAZA and NCBI transcriptome sequences.

        Parameters
        ----------
        blast_binary : str
            path to the blastn binary
        ref_name : str
            name of the reference species
        plaza_file_name : str
            name of the file with PLAZA transcripts
        ncbi_file_name : str
            name of the file with NCBI transcripts

        Returns
        -------
        str
            name of the file containing BLAST alignment result
        """
        args = f"gunzip -f {plaza_file_name} {ncbi_file_name}".split()
        popen = subprocess.Popen(args, stdout=subprocess.PIPE)
        popen.wait()

        tools.print_info_message("Performing blastn alignment...", 2)
        out_file_name = f"{os.path.join(self.tmp_dir, 'align__blast__' + ref_name)}.tsv"
        args = f"{blast_binary} -query {ncbi_file_name[:len(ncbi_file_name) - 3]} -subject {plaza_file_name[:len(plaza_file_name) - 3]} -outfmt 6 -out {out_file_name}".split()
        popen = subprocess.Popen(args, stdout=subprocess.PIPE)
        popen.wait()
        return out_file_name
    
    
    def _import_unlinked_plaza_genes(self, plaza_spec_name: str, linked_gene_identifiers: List):
        """
        If some genes provided by PLAZA couldn't be linked to the NCBI gene identifiers, they are imported separately.
        For genes that were already linked, PLAZA synonyms are updated.
        Information about PLAZA genes is retrieved from the "id_conversion" file containing the provided PLAZA species name.

        Parameters
        ----------
        plaza_spec_name : str
            short species name description used by PLAZA
        linked_gene_identifiers : List
            list of PLAZA gene identifiers that could be linked to NCBI identifiers
        """
        matching_files = [file_name for file_name in self.plaza_file_names if f".{plaza_spec_name}." in file_name]
        if len(matching_files) != 1:
            tools.print_warning_message(f"Found {len(matching_files)} PLAZA files instead of 1 for the species '{plaza_spec_name}'. Unlinked genes could not be imported.")
            if len(matching_files > 1):
                tools.print_warning_message(f"The matching files are: {matching_files}")
            return
        try:
            plaza_file_name = matching_files[0]
            tools.print_info_message(f"Reading file '{plaza_file_name}'...", 2)
            with gzip.open(plaza_file_name, 'rb') as plaza_file:
                next(plaza_file)  # PLAZA instance
                next(plaza_file)  # File generation timestamp
                next(plaza_file)  # Species information
                next(plaza_file)  # - species
                next(plaza_file)  # - common name
                tax_id = int(next(plaza_file).decode("utf-8").split(' : ')[-1].strip())  # - tax id : XXXX
                source_version = next(plaza_file).decode("utf-8").split(' : ')[-1].strip()  # - assembly/annotation source/version : XXXX vXX
                data_provider = next(plaza_file).decode("utf-8").split(' : ')[-1].strip()  # - annotation data provider : https://XXXX
                next(plaza_file)  # gene_id    id_type    id
                
                gene_ids = {}
                for line in plaza_file:
                    line_parts = line.decode("utf-8").split('\t')
                    gene_name = line_parts[0].strip()
                    gene_synonym = line_parts[2].strip()
                    if gene_name in gene_ids:
                        gene_ids[gene_name]['synonyms'] += f" | {gene_synonym}"
                    else:
                        gene_ids[gene_name] = {'tax_id' : tax_id, 'db_xref' : f"{data_provider} - {source_version}", 'synonyms' : gene_synonym}
                        
            unlinked_genes = {key : value for key, value in gene_ids.items() if key not in linked_gene_identifiers}
            linked_genes = {key : value for key, value in gene_ids.items() if key in linked_gene_identifiers}
            tools.print_info_message(f"Found {len(unlinked_genes):,} unlinked genes out of {len(gene_ids):,}, importing to the database...", 2)
            self.db_handler.import_unlinked_plaza_genes(unlinked_genes)
            self.db_handler.update_linked_plaza_genes(linked_genes)
        except Exception as exception:
            tools.print_exception_message(f"Exception occurred while importing uninked PLAZA genes for '{plaza_spec_name}': {exception}.")
