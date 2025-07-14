"""
Class allowing to get/propagate parental term ids for the provided ontologies.
"""

from typing import List

import pandas as pd

from parsers.ontology.go_parser import GeneOntologyParser
from parsers.ontology.to_parser import TraitOntologyParser
from parsers.ontology.ppto_parser import PlantPhenotypeTraitOntologyParser
from tools import tools


class OntologyTermPropagator:
    """
    Class allowing to get/propagate parental term ids for the provided ontologies.
    """

    def __init__(self, to_parser: TraitOntologyParser, go_parser: GeneOntologyParser, ppto_parser: PlantPhenotypeTraitOntologyParser, verbose: bool = True) -> None:
        """
        Constructor

        Parameters
        ----------
        to_parser : TraitOntologyParser
            allows to handle plant trait ontology
        go_parser : GeneOntologyParser
            allows to handle gene ontology
        pptoo_parser : PlantPhenotypeTraitOntologyParser
            allows to handle plant phenotype and trait ontology
        verbose : bool, optional
            indicates when log messages should be printed; bu default True
        """
        self.to_parser = to_parser
        self.go_parser = go_parser
        self.ppto_parser = ppto_parser
        self.verbose = verbose


    def get_child_trait_ids(self, trait_id: str):
        """
        Returns child term ids of the provided term.

        Parameters
        ----------
        trait_id : str
            _description_

        Returns
        -------
        _type_
            _description_
        """
        if trait_id[:3] == "TO:":
            child_trait_ids = self.to_parser.get_term_descendants(trait_id)
        elif trait_id[:3] == "GO:":
            child_trait_ids = self.go_parser.get_term_descendants(trait_id)
        else:
            child_trait_ids = self.ppto_parser.get_term_descendants(trait_id)
        return child_trait_ids


    def get_parental_trait_ids(self, trait_id: str):
        """
        Returns parental term ids of the provided term id.

        Parameters
        ----------
        trait_id : str
            a term id

        Returns
        -------
        List
            list containing parental term ids of the provided term id
        """
        par_trait_ids = []
        try:
            if trait_id[:3] == "TO:":
                par_trait_ids = self.to_parser.get_term_ancestors(trait_id)
            elif trait_id[:3] == "GO:":
                par_trait_ids = self.go_parser.get_term_ancestors(trait_id)
            else:
                par_trait_ids = self.ppto_parser.get_term_ancestors(trait_id)
        except Exception as exception:
            tools.print_warning_message(f"Error occurred while searching parental terms of {trait_id}.")
        finally:
            return par_trait_ids


    def propagate_parental_terms(self, associations_df: pd.DataFrame, species_gene_count: int = -1, generic_term_perc_threshold: int = -1):
        """
        Updates the provided associations dataframe with propagated parental terms.
        The first two columns of the input dataframe must be 'term id' and 'gene id'.
        If generic_term_perc_threshold is specified, it will be used to filter out
        too generic terms (associated to a high number of genes).

        Parameters
        ----------
        associations_df : pd.DataFrame
            dataframe with term-gene associations
        species_gene_count : int, optional
            total number of coding genes in species, by default -1
        generic_term_perc_threshold : int, optional
            the percentage of term-gene associations allowed, compared to the total number of genes, by default -1

        Returns
        -------
        pd.DataFrame
            dataframe with original associations and propagated parental terms
        """
        propagated_terms = []
        associations_df.apply(self._propagate_parental_terms_on_row, args=(propagated_terms,), axis=1)
        propagated_terms_df = pd.DataFrame(propagated_terms, columns=associations_df.columns).drop_duplicates()
        result_df = pd.concat([associations_df, propagated_terms_df], ignore_index=True)
        if species_gene_count != -1 and generic_term_perc_threshold != -1:
            if generic_term_perc_threshold < 0 or generic_term_perc_threshold > 100:
                raise Exception(f"Invalid percentage threshold provided for parental term propagation: {generic_term_perc_threshold}")
            max_number_of_genes_per_term = species_gene_count / 100 * generic_term_perc_threshold
            result_df = result_df[result_df.groupby('trait_id')['trait_id'].transform('size') <= max_number_of_genes_per_term]
        return result_df


    def _propagate_parental_terms_on_row(self, row: pd.Series, propagated_terms: List):
        """
        Propagates parental terms of one row of associations dataframe.

        Parameters
        ----------
        row : pd.Series
            series in form ['trait_id', 'gene_id', [...]]
        propagated_terms : List
            list of propagated terms computed so far
        """
        par_trait_ids = self.get_parental_trait_ids(row[0])
        for par_trait_id in par_trait_ids:
            new_row = row.copy()
            new_row[0] = par_trait_id
            propagated_terms.append(new_row.to_dict())
