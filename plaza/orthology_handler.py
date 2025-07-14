"""
Provides orthology information retrieved from PLAZA.
Three orthology sources are imported: tree-based, gene family and best-hits-and-inparalogs.
"""

import gzip
import os

import pandas as pd

from tools import tools
from tools.database_handler import DatabaseHandler
from tools.exceptions import FileFormatError


class OrthologyHandler:
    """
    Provides orthology information retrieved from PLAZA.
    Three orthology sources are imported: tree-based, gene family and best-hits-and-inparalogs.
    """

    def __init__(self, db_handler: DatabaseHandler, species_list_file_name: str, max_ortho_links: int, verbose: bool = True):
        """
        Constructor

        Parameters
        ----------
        db_handler : DatabaseHandler
            database handler, used to perform database requests
        species_list_file_name : str
            list of species for which orthologous information is retrieved from PLAZA
        max_ortho_links : int
            keep at most X orthologous links to keep (1-to-1, 1-to-2, ..., 1-to-X)
        verbose : bool, optional
            indicates whether more detailed log messages should be shown, by default True
        """
        self.db_handler = db_handler
        try:
            self.max_ortho_links = int(max_ortho_links)
        except ValueError:
            tools.print_exception_message(f"Invalid value for max_ortho_links: '{max_ortho_links}'!")
            sys.exit(1)
        self.verbose = verbose
        self.species_dict = self._parse_species_file(species_list_file_name)  # in form dict[short_plaza_id] = tax_id


    def _parse_species_file(self, species_to_import_file_name: str):
        """
        Parses the file specifying species for which orthology information is retrieved
        and creates a dictionary linking PLAZA short species names to their tax ids.

        Parameters
        ----------
        species_to_import_file_name : str
            name of the file with the list of species to import

        Returns
        -------
        Dict
            dictionary linking parsed short PLAZA species names to their tax ids

        Raises
        ------
        FileFormatError
            raised when the input file is incorrect
        """
        if self.verbose:
            tools.print_info_message("RETRIEVING SPECIES TO IMPORT...", 0)
        species_to_import_dict = {}
        items_per_line = 3
        with open(species_to_import_file_name, encoding="utf-8") as species_to_import_file:
            for line in species_to_import_file:
                if not line.startswith('#'):  # comment
                    line_parts = line.split('\t')
                    if len(line_parts) != items_per_line:
                        raise FileFormatError(f"Incorrect number of elements per line in the document '{species_to_import_file_name}': {len(line_parts)} instead of {items_per_line}.")
                    tax_id = line_parts[1]
                    plaza_name = line_parts[2].strip()
                    species_to_import_dict[plaza_name] = tax_id
        if self.verbose:
            tools.print_info_message(f"Retrieved {len(species_to_import_dict):,} species.")
        return species_to_import_dict


    def import_orthologies(self, out_dir: str, tree_based_url: str, gene_family_url: str, bhif_url: str):
        """
        Imports the three orthologies to the database.

        Parameters
        ----------
        species_to_import_file_name : str
            name of the file with the list of species to import
        out_dir : str
            directory used to store the downloaded and computed data
        tree_based_url : str
            URL of the tree-based orthology file
        gene_family_url : str
            URL of the gene family orthology file
        bhif_url : str
            URL of the BHIF orthology file
        """
        tools.print_info_message("IMPORTING ORTHOLOGY FILES FROM PLAZA...", 0)
        self._import_one_orthology(tree_based_url, "tree_based", out_dir)
        self._import_one_orthology(gene_family_url, "gene_family", out_dir)
        self._import_one_orthology(bhif_url, "bhif", out_dir)
        tools.print_info_message("Done.")


    def _import_one_orthology(self, ortho_url: str, ortho_name: str, out_dir: str, ):
        """
        Imports the provided orthology to the database.

        Parameters
        ----------
        ortho_url : str
            URL of the orthology to import
        ortho_name : str
            orthology name
        out_dir : str
            directory for downloaded files
        """
        if self.verbose:
            tools.print_info_message(f"Importing {ortho_name} orthology...")
        
        ortho_file_name = os.path.join(out_dir, f"{ortho_name}.csv.gz")
        tools.download_url(ortho_url, ortho_file_name, self.verbose)
        
        try:
            with gzip.open(ortho_file_name, 'rb') as ortho_file:
                next(ortho_file)  # PLAZA instance
                next(ortho_file)  # File generation timestamp
                next(ortho_file)  # header

                values = []
                parsed_cnt = 0
                imported_cnt = 0
                for line in ortho_file:
                    line_parts = line.decode("utf-8").split('\t')
                    query_gene_name = line_parts[0]
                    query_species = line_parts[1]
                    ortho_gene_name = line_parts[2]
                    ortho_species = line_parts[3].strip()
                    parsed_cnt += 1

                    if query_species in self.species_dict and ortho_species in self.species_dict:
                        query_species_id = self.species_dict[query_species]
                        ortho_species_id = self.species_dict[ortho_species]
                        values.append((query_species_id, query_gene_name, ortho_species_id, ortho_gene_name, ortho_name))
                        imported_cnt += 1

                    if len(values) > 0 and imported_cnt % 10_000 == 0:
                        self.db_handler.import_plaza_orthology(values)
                        values = []
                        if self.verbose and imported_cnt % 1_000_000 == 0:
                            tools.print_info_message(f"Imported {imported_cnt:,} out of {parsed_cnt:,} entries so far...", 2)

                if len(values) > 0:
                    self.db_handler.import_plaza_orthology(values)
                    
                tools.print_info_message(f"Parsed {parsed_cnt:,} entries, imported {imported_cnt:,} entries.", 2)
        except Exception as exception:
            tools.print_exception_message(f"Error has occurred during PLAZA identifiers analysis: {exception}")


    def get_available_tax_ids(self):
        """
        Provides the list of tax ids for which orthology information is available.

        Returns
        -------
        _type_
            _description_
        """
        return [int(tax_id) for tax_id in self.species_dict.values()]


    def get_orthology_genes_for_species(self, query_tax_id: int, ortho_tax_id: int):
        """
        Provides a dataframe with orthologous genes between the query species and the orthologous species.
        The genes are kept if their orthologous relationship is confirmed by at least 2 orthologous methods
        (tree-based, orthologous gene family or best-hits-and-inparalogs).

        Parameters
        ----------
        query_tax_id : int
            tax id of the query species
        ortho_tax_id : int
            tax id of the orthologous species

        Returns
        -------
        pd.DataFrame
            dataframe with orthologous genes
        """
        all_ortho_links = self.db_handler.get_ortho_links_between_species(query_tax_id, ortho_tax_id)
        column_names = ['query_gene', 'ortho_gene', 'ortho_link', 'query_gene_id', 'query_gene_synonyms']
        if len(all_ortho_links) > 0:
            all_ortho_links_df = pd.DataFrame(all_ortho_links).transpose()
            all_ortho_links_df.columns = column_names
            # only keep links confirmed by more than one orthologous method
            filtered_links = all_ortho_links_df.groupby(['query_gene', 'ortho_gene', 'query_gene_id', 'query_gene_synonyms'])['ortho_link'].count()[lambda x: x > 1]  # selection of gene pairs with more than one orthologous link
            filtered_links_df = filtered_links.reset_index().drop('ortho_link', axis=1)
            # filter based on max_ortho_links
            result = self.remove_ortho_duplicates(filtered_links_df)
        else:
            result = pd.DataFrame(columns=column_names)
        return result


    def remove_ortho_duplicates(self, ortho_links_df: pd.DataFrame):
        # we only want 1-to-X ortho links, so remove all the genes occurring multiple times in the query species
        filtered_links_df = ortho_links_df.drop_duplicates(subset=['query_gene'], keep=False)

        # count the number of times each ortho gene occurs
        gene_counts = filtered_links_df["ortho_gene"].value_counts()

        # keep only genes that appear at most max_ortho_links times
        genes_to_keep = gene_counts[gene_counts <= self.max_ortho_links].index
        
        # filter the dataframe
        filtered_links_df = filtered_links_df[filtered_links_df["ortho_gene"].isin(genes_to_keep)]

        return filtered_links_df
    

    def get_orthologous_triples(self, tax_id: int, function_providing_triples_for_tax_id):
        """
        Provides a dataframe containing triples for orthologous genes of the provided species.

        Parameters
        ----------
        tax_id : int
            tax_id of the original species
        function_providing_triples_for_tax_id : method
            function that will provide triples for orthologous tax ids

        Returns
        -------
        pd.DataFrame
            dataframe with triples from orthologous genes
        """
        full_ortho_df = pd.DataFrame()
        query_tax_ids = self.get_available_tax_ids()
        for query_tax_id in query_tax_ids:
            if query_tax_id != tax_id:  # don't do it for the species itself
                # step 1: retrieve association evidences for the species, one of the columns must be 'gene_name'
                ortho_associations_df = function_providing_triples_for_tax_id(query_tax_id, True)
                if not ortho_associations_df.empty:
                    # step 2: retrieve orthology information for the species of interest
                    ortho_df = self.get_orthology_genes_for_species(tax_id, query_tax_id)
                    # step 3: only keep information for orthologous genes and add gene names of the species of interest
                    ortho_df.rename(columns={"ortho_gene" : "gene_name"}, inplace=True)
                    merged_df = pd.merge(ortho_associations_df, ortho_df, how="inner", on=["gene_name"])
                    # step 4: add the computed orthology information to the original dataframe
                    merged_df['ortho_species_id'] = query_tax_id
                    merged_df.insert(len(merged_df.columns)-1, 'ortho_gene_name', merged_df.pop('gene_name'))
                    merged_df.pop('gene_id')
                    merged_df.pop('gene_synonyms')
                    merged_df.insert(1, 'gene_synonyms', merged_df.pop('query_gene_synonyms'))
                    merged_df.insert(1, 'gene_name', merged_df.pop('query_gene'))
                    merged_df.insert(1, 'gene_id', merged_df.pop('query_gene_id'))
                    if self.verbose:
                        tools.print_info_message(f"After orthology transfer: {len(merged_df[['trait_id', 'gene_id']].drop_duplicates()):,} triples left.", 4)
                    full_ortho_df = pd.concat([full_ortho_df, merged_df], ignore_index=True)
        # set the species id of the original species
        full_ortho_df['species_id'] = tax_id
        return full_ortho_df
