"""
Reads the NCBI gene identifier file and creates the corresponding gene identifier dictionary.
FTP: https://ftp.ncbi.nih.gov/gene/DATA/
File: gene_info.gz
"""

import csv
import os

from parsers.generic_parser import GenericParser
from tools import tools


class NcbiGeneIdentifierParser(GenericParser):
    """
    Reads the NCBI gene identifier file and creates the corresponding gene identifier dictionary.
    FTP: https://ftp.ncbi.nih.gov/gene/DATA/
    File: gene_info.gz
    File structure: tab delimited with following columns:
        [0] [tax id] the unique identifier provided by NCBI taxonomy for the species or strain/isolate
        [1] [gene id] the unique identifier for a gene
        [2] [symbol] the default symbol for the gene
        [3] [locus tag] the locus tag value
        [4] [synonyms] bar-delimited set of unofficial symbols for the gene
        [5] [dbXref] bar-delimited set of identifiers in other databases for this gene (database:value)
        [6] [chromosome] the chromosome on which this gene is placed (M for mitochonrdrial)
        [7] [map location] the map location for this gene
        [8] [description] a descriptive name for this gene
        [9] [type of gene] the type assigned to the gene according to the list of options provided on a website
        [10] [symbol from nomenclature authority] when not '-', indicates that this symbol is from a nomenclature authority
        [11] [nomenclature status] when not '-', indicates the status of the name from the nomenclature authority (O for official, I for interim)
        [12] [other designations] pipe-delimited set of some alternate descriptions that have ben assigne to a gene id
        [13] [modification date] the last date a gene record was updated, in YYYYMMDD format
        [14] [feature type] pipe-delimited set of annotated features and their classes of controlled vocabularies, displayed as feature_type:feature_class
    Parsed columns: [0, 1, 2, 3, 4, 5]
    """

    def __init__(self, gene_info_url: str, tmp_dir: str, verbose: bool = True) -> None:
        """
        Parses the provided URL and creates the gene identifier dictionary.
        The dictionary is in the form dict[id] = list[synonym].

        Parameters
        ----------
        gene_info_url : str
            URL of the NCBI gene_info file
        tmp_dir : str
            directory for temporary files
        verbose : bool, optional
            indicates when log messages should be printed; bu default True
        """

        self._ncbi_gene_info_url = gene_info_url
        self.tmp_dir = tmp_dir

        GenericParser.__init__(self, verbose=verbose)
    

    def parse_files(self):
        """
        Parses the file gene_info.gz and constructs the corresponding gene dictionary.
        """
        try:
            tools.print_info_message("IMPORTING GENE IDENTIFIERS...", 0)

            item_cnt = 0
            ncbi_gene_info_file_name = os.path.join(self.tmp_dir, "gene_info.tsv")
            tools.download_and_extract_gz_file(self._ncbi_gene_info_url, ncbi_gene_info_file_name, True)
            tools.print_info_message(f"Parsing gene identifiers...")
            with open(ncbi_gene_info_file_name, encoding="utf8") as ncbi_gene_info_file:
                next(ncbi_gene_info_file)  # the first row is a header
                reader = csv.reader(ncbi_gene_info_file, delimiter='\t')
                for row in reader:
                    item_cnt += 1
                    tax_id = row[0]
                    key = row[1]
                    symbol = row[2]
                    locus_tag = row[3]
                    db_xref = row[5]
                    self.dictionary[key] = { 'tax_id' : tax_id, 'synonyms' : symbol, 'symbol' : symbol, 'locus_tag' : locus_tag, 'db_xref' : db_xref }
                    synonyms = row[4]
                    if synonyms != '-':
                        self.dictionary[key]['synonyms'] += " | " + synonyms.replace('|', ' | ')
            if self._verbose:
                tools.print_info_message(f"Parsed {item_cnt:,} gene identifiers.", 2)
        except Exception as exception:
            tools.print_exception_message(f"NCBI gene identifiers could not be parsed: {exception}")
