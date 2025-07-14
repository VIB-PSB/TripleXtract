"""
Computes various statistics and plots graphs in the specified folder.
"""

import math
import os
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import pandas as pd
from pandas.core.frame import DataFrame
import numpy as np
from scipy.stats import gaussian_kde
import seaborn as sb
import upsetplot as ups

import export.export_tools as export_tools
from tools import tools, constants
from tools.database_handler import DatabaseHandler
from tools.ontology_term_propagator import OntologyTermPropagator  # pylint: disable=ungrouped-imports


class StatsExtractor():
    """
    Computes various statistics and plots graphs in the specified folder.
    """

    def __init__(self, db_handler: DatabaseHandler, ontology_term_propatagor: OntologyTermPropagator, out_dir: str):
        """
        Constructor

        Parameters
        ----------
        db_handler : DatabaseHandler
            allows to perform database queries
        ontology_term_propatagor : OntologyTermPropagator
            allows to propagate ontology parental terms
        out_dir : str
            directory where the output files will be stored
        """
        self.db_handler = db_handler
        self.ontology_term_propagator = ontology_term_propatagor
        self.out_dir = out_dir
        self.selected_species = {3702: "A. thaliana", 4577: "Z. mays", 4530: "O. sativa", 4565: "T. aestivum", 4081: "S. lycopersicum", 4113: "S. tuberosum"}        
        self.selected_to_traits = {"TO:0000207": "plant height", "TO:0000228": "moisture content", "TO:0000291": "carbohydrate content", "TO:0000396": "grain yield", "TO:0000399": "grain thickness",
                                  "TO:0000683": "ear infructescence position", "TO:0000734": "grain length", "TO:0000753": "coleoptile morphology", "TO:0000871": "fruit yield", "TO:0000894": "fruit number",
                                  "TO:0000906": "exocarp morphology", "TO:0000919": "grain weight", "TO:0000936": "infructescence yield", "TO:0000975": "grain width", "TO:0002616": "flowering time",
                                  "TO:0002626": "fruit length", "TO:0002627": "fruit width", "TO:0002629": "fruit morphology", "TO:0002759": "grain number", "TO:0006007": "polysaccharide content"}
        self.selected_go_traits = {"GO:0008219": "cell death", "GO:0009408": "response to heat", "GO:0009409": "response to cold", "GO:0009414": "response to water deprivation", "GO:0009611": "response to wounding",
                                   "GO:0009635": "response to herbicide", "GO:0009908": "flower development", "GO:0010154": "fruit development", "GO:0010254": "nectary development", "GO:0010338": "leaf development",
                                  "GO:0035266": "meristem growth", "GO:0048316": "seed development", "GO:0048364": "root development", "GO:0048440": "carpel development", "GO:0048441": "petal development",
                                  "GO:0048481": "plant ovule development", "GO:0048527": "lateral root development", "GO:0080022": "primary root development", "GO:1901698": "response to nitrogen compound", "GO:1905328": "plant septum development"}
        

    def print_statistics(self):
        tools.print_info_message("Computing statistics...", 0)

        tools.print_info_message("===== TRIPLES ==================", 2)
        all_tm_triples = self.db_handler.get_number_of_tm_triples()
        all_tm_species = self.db_handler.get_number_of_tm_species()
        all_tm_genes = self.db_handler.get_number_of_tm_genes()
        all_tm_traits = self.db_handler.get_number_of_tm_traits()
        tools.print_info_message(f"{'ALL':<8} --> triples: {all_tm_triples:>9,} - S: {all_tm_species:>3,} - G: {all_tm_genes:>6,} - T: {all_tm_traits:>5,}", 2)
        retained_tm_triples = self.db_handler.get_number_of_retained_tm_triples()
        retained_tm_species = self.db_handler.get_number_of_retained_tm_species()
        retained_tm_genes = self.db_handler.get_number_of_retained_tm_genes()
        retained_tm_traits = self.db_handler.get_number_of_retained_tm_traits()
        tools.print_info_message(f"{'RETAINED':<8} --> triples: {retained_tm_triples:>9,} - S: {retained_tm_species:>3,} - G: {retained_tm_genes:>6,} - T: {retained_tm_traits:>5,}", 2)
        
        tools.print_info_message("===== ONTOLOGY STATS ============", 2)
        all_to_triples = self.db_handler.get_number_of_to_term_triples()
        all_go_triples = self.db_handler.get_number_of_go_term_triples()
        all_ppto_triples = self.db_handler.get_number_of_ppto_term_triples()
        tools.print_info_message(f"{'ALL TRIPLES':<11} --> TO: {all_to_triples:>7,} - GO: {all_go_triples:>9,} - PPTO: {all_ppto_triples:>7,}", 2)
        all_to_terms = self.db_handler.get_number_of_to_terms()
        all_go_terms = self.db_handler.get_number_of_go_terms()
        all_ppto_terms = self.db_handler.get_number_of_ppto_terms()
        tools.print_info_message(f"{'ALL TERMS':<11} --> TO: {all_to_terms:>7,} - GO: {all_go_terms:>9,} - PPTO: {all_ppto_terms:>7,}", 2)
    
    
    def print_per_species_statistics(self):
        tools.print_info_message("Computing per species statistics...", 0)

        retained_tax_ids = self.db_handler.get_retained_species()

        for retained_tax_id in retained_tax_ids:
            retained_tax_id = retained_tax_id[0]
            species_name = self.db_handler.get_species_name_from_tax_id(retained_tax_id)
            tools.print_info_message(f"{species_name.split('|')[0]} -- tax id {retained_tax_id}", 2)
    
            tm_genes_tax_id = self.db_handler.get_number_of_retained_tm_genes_for_tax_id(retained_tax_id)
            tm_traits_tax_id = self.db_handler.get_number_of_retained_tm_traits_for_tax_id(retained_tax_id)
            tm_triples_tax_id = self.db_handler.get_number_of_retained_tm_triples_for_tax_id(retained_tax_id)
            tools.print_info_message(f"Text mining    --> G: {tm_genes_tax_id:>6,} - T: {tm_traits_tax_id:5,} - triples: {tm_triples_tax_id:>7,}", 3)


    
    def draw_max_score_vs_evidences(self):
        """
        Draws a scatter plot with densities for TM triples, comparing their maximum score to their number of evidences.
        """
        tools.print_info_message("Plotting max score vs number of evidences for TM triples...")
        
        distrib = self.db_handler.get_all_tm_evidences()

        # taken from https://stackoverflow.com/questions/20105364/how-can-i-make-a-scatter-plot-colored-by-density-in-matplotlib/20107592#20107592
        # pylint: disable=invalid-name
        xy = np.vstack([distrib[2], distrib[3]])
        z = gaussian_kde(xy)(xy)
        idx = z.argsort()  # to sort values by density, so the points with the highest density are plot at the end
        x, y, z = np.array(distrib[2])[idx], np.array(distrib[3])[idx], z[idx]
        fig, ax = plt.subplots()
        cax = ax.scatter(x, y, c=z, s=100)
        fig.colorbar(cax, label="Density")

        self.set_plot_properties("Max score vs number of evidences", "Max score", "Number of evidences", filename="scat__max-score-vs-number-of-evidences.png")
        tools.print_info_message("Done.")


    def draw_bar_associations_per_case(self):
        """
        Draws a bar plot showing associations per case distribution.
        """
        tools.print_info_message("Extracting associations per type distribution...")
        distrib = self.db_handler.extract_associations_per_case()

        self.draw_bar(distrib[0], distrib[1])
        self.set_plot_properties("Associations per case", "Cases", "Associations", filename="bar__associations-per-case.png")
        tools.print_info_message("Done.")


    def draw_bar_assoc_score_per_section_type(self):
        """
        Draws bar plots showing mean and median association score per section type.
        """
        tools.print_info_message("Extracting mean and median evidence scores...")
        sections = ["TITLE", "ABSTRACT", "INTRO", "RESULTS", "DISCUSS", "CONCL", "METHODS"]
        mean_scores = {}
        median_scores = {}
        mean_scores_no_d = {}
        median_scores_no_d = {}
        for section in sections:
            tools.print_info_message(f"--> for section {section}...")
            assoc_scores = self.db_handler.extract_tm_evidence_scores_per_section_type(section)
            assoc_scores_df = pd.DataFrame(assoc_scores, columns=['assoc_id', 'type_id', 'score'])
            mean_scores[section] = assoc_scores_df['score'].mean()
            median_scores[section] = assoc_scores_df['score'].median()
            
            assoc_scores_no_d = self.db_handler.extract_tm_evidence_scores_per_section_type(section, without_d_cases = True)
            assoc_scores_no_d_df = pd.DataFrame(assoc_scores_no_d, columns=['assoc_id', 'type_id', 'score'])
            mean_scores_no_d[section] = assoc_scores_no_d_df['score'].mean()
            median_scores_no_d[section] = assoc_scores_no_d_df['score'].median()

        max_score = 100
        self.draw_bar(mean_scores.keys(), mean_scores.values(), subplot=1)
        self.set_plot_properties("All cases", "Section type", "Mean score", y_scale_is_log=False, max_ylim=max_score, x_labels_vertical=True)
        self.draw_bar(mean_scores_no_d.keys(), mean_scores_no_d.values(), subplot=2)
        self.set_plot_properties("Without d", "Section type", "", y_scale_is_log=False, max_ylim=max_score, x_labels_vertical=True, filename="bar__mean-assoc-score-per-section-type.png")

        self.draw_bar(median_scores.keys(), median_scores.values(), subplot=1)
        self.set_plot_properties("All cases", "Section type", "Median score", y_scale_is_log=False, max_ylim=max_score, x_labels_vertical=True)
        self.draw_bar(median_scores_no_d.keys(), median_scores_no_d.values(), subplot=2)
        self.set_plot_properties("Without d", "Section type", "", y_scale_is_log=False, max_ylim=max_score, x_labels_vertical=True, filename="bar__median-assoc-score-per-section-type.png")
        tools.print_info_message("Done.")


    def draw_bar_publications_per_year(self):
        """
        Draws a bar plot showing publications per year distribution.
        """
        tools.print_info_message("Extracting publications per year distribution...")
        distribution = self.db_handler.extract_publications_per_year()
        self.draw_bar(distribution[0], distribution[1])
        self.set_plot_properties("Publications per year", "Year", "Publications", y_scale_is_log=True, filename="bar__publications-per-year.png")
        tools.print_info_message("Done.")


    def draw_bar_unique_triples_per_trait(self):
        """
        Draws a bar plot showing unique triples per trait and per source.
        """
        tools.print_info_message("Extracting unique triples per trait and per source...")
        title = "Unique triples per trait and source"
        self._draw_bar_unique_triples_per_trait(self.selected_to_traits, f"{title} - TO (all species)", "bar__unique-triples-per-trait-to.png")
        self._draw_bar_unique_triples_per_trait(self.selected_go_traits, f"{title} - GO (all species)", "bar__unique-triples-per-trait-go.png")
        self._draw_bar_unique_triples_per_trait(self.selected_to_traits, f"{title} - TO ($A. thaliana$)", "bar__unique-triples-per-trait-to__ath.png", 3702)
        self._draw_bar_unique_triples_per_trait(self.selected_go_traits, f"{title} - GO ($A. thaliana$)", "bar__unique-triples-per-trait-go__ath.png", 3702)
        self._draw_bar_unique_triples_per_trait(self.selected_to_traits, f"{title}- TO ($Z. mays$)", "bar__unique-triples-per-trait-to__zma.png", 4577)
        self._draw_bar_unique_triples_per_trait(self.selected_go_traits, f"{title} - GO ($Z. mays$)", "bar__unique-triples-per-trait-go__zma.png", 4577)
        self._draw_bar_unique_triples_per_trait(self.selected_to_traits, f"{title} - TO ($O. sativa$)", "bar__unique-triples-per-trait-to__osa.png", 39947)
        self._draw_bar_unique_triples_per_trait(self.selected_go_traits, f"{title} - GO ($O. sativa$)", "bar__unique-triples-per-trait-go__osa.png", 39947)
        tools.print_info_message("Done.")


    def _draw_bar_unique_triples_per_trait(self, selected_traits: Dict, title: str, filename: str, species_id: int = -1):
        """
        Draws a bar plot showing unique triples per trait and per source.
        If species id is specified, only extracts data for that species.

        Parameters
        ----------
        title : str
            title of the graph
        filename : str
            file name
        species_id : int, optional
            species tax id, by default -1
        """
        trait_cnt = len(selected_traits)
        distribution_df = pd.DataFrame({'trait_id': list(selected_traits.keys()), 'trait_name': list(selected_traits.values()), 'Text mining': [0]*trait_cnt}).set_index('trait_id')
        for trait_id in selected_traits.keys():
            child_trait_ids = self.ontology_term_propagator.get_child_trait_ids(trait_id)
            for a_trait_id in [trait_id] + child_trait_ids:
                if species_id == -1:
                    distribution_df.loc[trait_id, 'Text mining'] += self.db_handler.get_number_of_tm_evidences_for_trait(a_trait_id)
                else:
                    distribution_df.loc[trait_id, 'Text mining'] += self.db_handler.get_number_of_tm_evidences_for_trait_and_species(a_trait_id, species_id)
                    if species_id == 39947:
                        alternative_o_sativa_id = 4530  # for O. sativa, two ids are commonly used: take them both
                        distribution_df.loc[trait_id, 'Text mining'] += self.db_handler.get_number_of_tm_evidences_for_trait_and_species(a_trait_id, alternative_o_sativa_id)
        distribution_df['full_trait_name'] = distribution_df.apply(lambda row: f"{row.name} {row['trait_name']}", axis=1)
        distribution_df.set_index('full_trait_name', inplace=True)
        distribution_df.pop('trait_name')
        self.draw_bar_for_df(distribution_df, True, True)
        self.set_plot_properties(title, "Number of triples", "Traits", y_scale_is_log=False, filename=filename)


    def draw_hist_evidences_per_association(self):
        """
        Draws a histogram showing evidences per association distribution.
        Each histogram is drawn twise: for all association evidence cases and
        with cases 1d and 2d excluded.
        """
        tools.print_info_message("Extracting evidences per association distribution...")
        assoc_scores = self.db_handler.extract_tm_evidence_scores()
        assoc_scores_df = pd.DataFrame(assoc_scores, columns=['assoc_id', 'type_id', 'score'])
        del assoc_scores_df['type_id']

        assoc_scores_no_d = self.db_handler.extract_tm_evidence_scores(without_d_cases = True)
        assoc_scores_no_d_df = pd.DataFrame(assoc_scores_no_d, columns=['assoc_id', 'type_id', 'score'])
        del assoc_scores_no_d_df['type_id']

        ev_per_assoc = assoc_scores_df['assoc_id'].value_counts()
        ev_per_assoc_no_d = assoc_scores_no_d_df['assoc_id'].value_counts()
        max_x = max(max(ev_per_assoc),max(ev_per_assoc_no_d))
        max_y = max(max(ev_per_assoc.index),max(ev_per_assoc_no_d.index))
        self.draw_histogram(ev_per_assoc, 100, subplot=1)
        self.set_plot_properties("All cases", "Evidences", "Associations", max_xlim=max_x, max_ylim=max_y)
        self.draw_histogram(ev_per_assoc_no_d, 100, subplot=2)
        self.set_plot_properties("Without d cases", "Evidences", "", max_xlim=max_x, max_ylim=max_y, filename="hist__evidences_per_association_no_d.png")

        mean_ev_scores = assoc_scores_df.groupby('assoc_id').mean()
        mean_ev_scores_no_d = assoc_scores_no_d_df.groupby('assoc_id').mean()
        max_x = max(max(mean_ev_scores['score']),max(mean_ev_scores_no_d['score']))
        max_y = max(max(mean_ev_scores.index),max(mean_ev_scores_no_d.index))
        self.draw_histogram(mean_ev_scores, 15, subplot=1)
        self.set_plot_properties("All cases", "Mean evidence score", "Associations", max_xlim=max_x, max_ylim=max_y)
        self.draw_histogram(mean_ev_scores_no_d, 15, subplot=2)
        self.set_plot_properties("Without d cases", "Mean evidence score", "", max_xlim=max_x, max_ylim=max_y, filename="hist__mean_score_per_association.png")

        median_ev_scores = assoc_scores_df.groupby('assoc_id').median()
        median_ev_scores_no_d = assoc_scores_no_d_df.groupby('assoc_id').median()
        max_x = max(max(median_ev_scores['score']),max(median_ev_scores_no_d['score']))
        max_y = max(max(median_ev_scores.index),max(median_ev_scores_no_d.index))
        self.draw_histogram(median_ev_scores, 15, subplot=1)
        self.set_plot_properties("All cases", "Median evidence score", "Associations", max_xlim=max_x, max_ylim=max_y)
        self.draw_histogram(median_ev_scores_no_d, 15, subplot=2)
        self.set_plot_properties("Without d cases", "Median evidence score", "", max_xlim=max_x, max_ylim=max_y, filename="hist__median_score_per_association.png")

        tools.print_info_message("Done.")


    def draw_hist_associations_per_paper(self):
        """
        Draws a histogram showing associations per paper distribution.
        """
        tools.print_info_message("Extracting associations per paper distribution...")
        distrib = self.db_handler.extract_associations_per_paper()
        self.draw_histogram(distrib[1], 100)
        self.set_plot_properties("Associations per document", "Associations", "Documents", filename="hist__associations-per-document__all.png")

        data = {}
        assoc_types = self.db_handler.get_association_types()
        max_x = 0
        for assoc_type in assoc_types[1]:
            tools.print_info_message(f"--> for type {assoc_type}...")
            distrib = self.db_handler.extract_associations_per_paper_and_type(assoc_type)
            data[assoc_type] = distrib
            max_x = max(max_x, max(distrib[1]))

        for assoc_type, distrib in data.items():
            self.draw_histogram(distrib[1], 100)
            self.set_plot_properties(f"Associations per document: case {assoc_type}", "Associations", "Documents", max_xlim=max_x, filename=f"hist__associations-per-document__{assoc_type}.png")
        tools.print_info_message("Done.")


    def draw_hist_max_score_per_triple(self):
        """
        Computes the distribution of maximum score per triple and the amount of evidences per triple for different data sets (full data, Ath, Zma).
        """
        tools.print_info_message("Extracting max score per association...")
        
        distrib = self.db_handler.get_all_tm_evidences()
        distrib_df = pd.DataFrame(distrib).transpose()
        distrib_df.columns = ['trait_id', 'gene_id', 'max_score', 'ev_count']
        distrib_df['max_score'] = distrib_df['max_score'].astype('float')
        distrib_df['ev_count'] = distrib_df['ev_count'].astype('float')

        self.draw_histogram_cumulative(distrib_df['max_score'], 20, "Max score per triple - all species", "Score", "Count", False, filename="hist__max-score-per-triple__all.png")
        self.draw_histogram_cumulative(distrib_df['ev_count'], 50, "Max evidences count per triple - all species", "Evidences", "Count", True, filename="hist__max-ev-count-per-triple__all.png")
        self.draw_histogram_cumulative(distrib_df['ev_count'], 50, "Max evidences count per triple - all species (limit = 1000)", "Evidences", "Count", True, filename="hist__max-ev-count-per-triple__all__zoom.png", max_x_value=1000)

        to_distrib_df = distrib_df.loc[distrib_df['trait_id'].str.startswith('TO:')]
        self.draw_histogram_cumulative(to_distrib_df['max_score'], 20, "Max score per triple - all species - TO terms", "Score", "Count", False, filename="hist__max-score-per-triple__all-to.png")

        go_distrib_df = distrib_df.loc[distrib_df['trait_id'].str.startswith('GO:')]
        self.draw_histogram_cumulative(go_distrib_df['max_score'], 20, "Max score per triple - all species - GO terms", "Score", "Count", False, filename="hist__max-score-per-triple__all-go.png")

        ath_distrib = self.db_handler.get_tm_evidences_for_tax_id(constants.TAX_ID__ARABIDOPSIS_THALIANA)
        self.draw_histogram_cumulative(ath_distrib[2], 20, "Max score per triple - TM & gene id - A. thaliana", "Score", "Count", False, filename="hist__max-score-per-triple__ath.png")
        self.draw_histogram_cumulative(ath_distrib[3], 50, "Max evidences count per triple - TM & gene id - A. thaliana", "Evidences", "Count", True, filename="hist__max-ev-count-per-triple__ath.png")
        self.draw_histogram_cumulative(ath_distrib[3], 50, "Max evidences count per triple - TM & gene id - A. thaliana (limit = 1000)", "Evidences", "Count", True, filename="hist__max-ev-count-per-triple__ath__zoom.png", max_x_value=1000)

        zma_distrib = self.db_handler.get_tm_evidences_for_tax_id(constants.TAX_ID__ZEA_MAYS)
        self.draw_histogram_cumulative(zma_distrib[2], 20, "Max score per triple - TM & gene id - Z. mays", "Score", "Count", False, filename="hist__max-score-per-triple__zma.png")
        self.draw_histogram_cumulative(zma_distrib[3], 50, "Max evidences count per triple - TM & gene id - Z. mays", "Evidences", True, "Count", filename="hist__max-ev-count-per-triple__zma.png")
        self.draw_histogram_cumulative(zma_distrib[3], 50, "Max evidences count per triple - TM & gene id - Z. mays (limit = 1000)", "Evidences", "Count", True, filename="hist__max-ev-count-per-triple__zma__zoom.png", max_x_value=1000)

        tools.print_info_message("Done.")


    def draw_hm_species_per_section(self):
        """
        Draws a heatmap showing, per section type, how many documents have exactly one species,
        two species, three species etc.
        """
        tools.print_info_message("Extracting species distribution for whole documents...")
        sections = ["TITLE", "ABSTRACT", "INTRO", "RESULTS", "DISCUSS", "CONCL", "METHODS"]
        idx_range = 20
        species_in_documents = self.db_handler.extract_species_distribution_per_section_type()
        documents_df = pd.DataFrame(species_in_documents, columns=["doc_id", "all_documents"])
        count = np.histogram(documents_df["all_documents"], bins=list(range(1,idx_range+2)))
        full_df = pd.DataFrame({"WHOLE": count[0]})
        full_df.rename(index={0:1,1:2,2:3,3:4,4:5,5:6,6:7,7:8,8:9,9:10,10:11,11:12,12:13,13:14,14:15,15:16,16:17,17:18,18:19,19:20}, inplace=True)
        for section in sections:
            tools.print_info_message(f"--> for section {section}...")
            species_in_section = self.db_handler.extract_species_distribution_per_section_type(section)
            section_df = pd.DataFrame(species_in_section, columns=["doc_id", section])
            count = np.histogram(section_df[section], bins=list(range(1,idx_range+2)))
            full_df[section] = count[0]
        self.draw_heatmap(full_df)
        self.set_plot_properties("Species per section", xlabel="Section", ylabel="Specis", y_scale_is_log=False, filename="hm__spec-per-section.png")
        tools.print_info_message("Done.")


    def draw_hm_traits_per_species(self, limit: int):
        """
        Draws a heatmap showing, per trait, how many times it is present in different species.
        The heatmap is drawn for the selected number of top traits and top species.

        Parameters
        ----------
        limit : int
            matrix dimensions (limit of traits and species)
        """
        tools.print_info_message(f"Extracting top {limit} species vs traits counts...")
        top_species = self.db_handler.extract_top_species_in_assocs(limit)
        top_traits = self.db_handler.extract_top_traits_in_assocs(limit)

        spec_synonyms = []
        for all_spec_synonyms in top_species[1]:
            synonyms = all_spec_synonyms.split('|')
            spec_synonyms.append(synonyms[1].strip())

        trait_synonyms = []
        for all_trait_synonyms in top_traits[1]:
            synonyms = all_trait_synonyms.split('|')
            trait_synonyms.append(synonyms[1].strip())

        data = []
        for idx, spec_id in enumerate(top_species[0]):
            tools.print_info_message(f"--> species {spec_synonyms[idx]}...")
            row = [spec_synonyms[idx]]
            for trait_id in top_traits[0]:
                row.append(self.db_handler.get_number_of_assocs_for_species_and_trait(spec_id, trait_id)[0])
            data.append(row)
        
        trait_synonyms.insert(0, 'species')
        dataframe = pd.DataFrame(data, columns=trait_synonyms)
        dataframe.set_index('species', inplace=True)
        self.draw_heatmap(dataframe, add_annotations=False, colors="flare")
        self.set_plot_properties("Text mining: most abundant traits and species", "Trait", "Species", y_scale_is_log=False, filename="hm__spec-trait-associations.png")
        tools.print_info_message("Done.")


    def draw_hm_traits_per_species_selected(self):
        """
        Draws a heatmap showing, per trait, how many times it is present in different species.
        The heatmap is drawn for the selection of (TO or GO) traits and species.
        """
        self._draw_hm_traits_per_species_selected_traits(self.selected_to_traits, "Text mining: selected TO terms", "hm__spec-trait-associations-to-selected.png")
        self._draw_hm_traits_per_species_selected_traits(self.selected_go_traits, "Text mining: selected GO terms", "hm__spec-trait-associations-go-selected.png")


    def _draw_hm_traits_per_species_selected_traits(self, selected_traits: Dict, title: str, filename: str):
        """
        Draws a heatmap showing, per trait, how many times it is present in different species.
        The heatmap is drawn for the selection of the provided traits and species.

        Parameters
        ----------
        selected_traits : Dict
            dictionary of selected traits, in form "trait id : trait synonym"
        title : str
            title of the graph
        filename : str
            file name
        """
        tools.print_info_message("Computing heatmap of selected traits vs selected species...")

        data = []
        for spec_id, spec_name in self.selected_species.items():
            tools.print_info_message(f"--> species {spec_name}...")
            row = [spec_name]
            for trait_id in selected_traits.keys():
                row.append(self.db_handler.get_number_of_assocs_for_species_and_trait(spec_id, trait_id)[0])
                if spec_id == 4530:  # for O. sativa, two tax id are commonly used
                    row[len(row)-1] += self.db_handler.get_number_of_assocs_for_species_and_trait(39947, trait_id)[0]
                child_terms = self.ontology_term_propagator.get_child_trait_ids(trait_id)
                for child_trait_id in child_terms:
                    row[len(row)-1] += self.db_handler.get_number_of_assocs_for_species_and_trait(spec_id, child_trait_id)[0]
                    if spec_id == 4530:  # for O. sativa, two tax id are commonly used
                        row[len(row)-1] += self.db_handler.get_number_of_assocs_for_species_and_trait(39947, child_trait_id)[0]
            data.append(row)
        
        dataframe = pd.DataFrame(data, columns=['species'] + list(selected_traits.values()))
        dataframe.set_index('species', inplace=True)
        self.draw_heatmap(dataframe, add_annotations=False, colors="flare")
        self.set_plot_properties(title, "Trait", "Species", y_scale_is_log=False, filename=filename)
        tools.print_info_message("Done.")
        

    def draw_hm_associations_per_paragraph_type(self):
        """
        Draws a heatmap showing, per annotation type (species / gene / trait),
        how many annotations are present in each section type (title / abstract / intro / etc).
        """
        tools.print_info_message("Extracting paragraph type relevance information from database...")
        spec_occurrences = self.db_handler.extract_species_annotation_paragraph_types()
        gene_occurrences = self.db_handler.extract_gene_annotation_paragraph_types()
        trait_occurrences = self.db_handler.extract_trait_annotation_paragraph_types()
        hq_assoc_occurrences = self.db_handler.extract_association_paragraph_types(high_quality_only=True)
        assoc_occurrences = self.db_handler.extract_association_paragraph_types()

        spec_df = pd.DataFrame.from_records([dict(spec_occurrences)]).transpose()
        spec_df.columns = ['species']
        gene_df = pd.DataFrame.from_records([dict(gene_occurrences)]).transpose()
        gene_df.columns = ['genes']
        trait_df = pd.DataFrame.from_records([dict(trait_occurrences)]).transpose()
        trait_df.columns = ['traits']
        hq_assoc_df = pd.DataFrame.from_records([dict(hq_assoc_occurrences)]).transpose()
        hq_assoc_df.columns = ['hq assocs']
        assoc_df = pd.DataFrame.from_records([dict(assoc_occurrences)]).transpose()
        assoc_df.columns = ['all assocs']
        full_df = pd.concat([spec_df, gene_df, trait_df, hq_assoc_df, assoc_df], axis=1).sort_values(by='all assocs', ascending=False)
        self.draw_heatmap(full_df)
        self.set_plot_properties("Associations per paragraph", xlabel="", ylabel="Paragraph type", y_scale_is_log=False, filename="hm__par-types.png")
        tools.print_info_message("Done.")


    def draw_upset_association_cases(self):
        """
        Draws an upset plot displaying the distribution of associations into cases.
        """
        tools.print_info_message("Extracting association evidence cases from the database...")
        assocs_vs_cases_df = export_tools.get_assocs_vs_cases_df(self.db_handler)
        upset_df = assocs_vs_cases_df.groupby(assocs_vs_cases_df.columns.tolist(),as_index=True).size()
        upset = ups.UpSet(upset_df, sort_by='cardinality')
        upset_min = ups.UpSet(upset_df, sort_by='cardinality', max_subset_size = 1000)
        upset_max = ups.UpSet(upset_df, sort_by='cardinality', min_subset_size = min(1000, upset_df.max() / 2))
        self._draw_upset(upset, "upset_cases.png")
        self._draw_upset(upset_min, "upset_cases_min.png")
        self._draw_upset(upset_max, "upset_cases_max.png")
        tools.print_info_message("Done.")


    def draw_scatter(self, x_values: List, y_values: List, subplot: int = 0):
        """
        Draws a scatter plot.

        Parameters
        ----------
        x_values : list
            X values
        y_values : list
            Y values
        subplot : int, optional
            if 0: draw a full plot, if > 0: number of subplot, by default 0
        """
        if subplot in (0, 1):  # either this plot has no sub-plots, or this is the first subplot
            plt.clf()
        if subplot > 0:
            plt.subplot(1, 2, subplot)
        plt.scatter(x_values, y_values)
        plt.grid(visible=True, which='major', axis='y')


    def draw_bar(self, categories: list, values: list, subplot: int = 0):
        """
        Draws a bar plot.

        Parameters
        ----------
        categories : list
            categories
        values : list
            values
        subplot : int, optional
            if 0: draw a full plot, if > 0: number of subplot, by default 0
        """
        if subplot in (0, 1):  # either this plot has no sub-plots, or this is the first subplot
            plt.clf()
        if subplot > 0:
            plt.subplot(1, 2, subplot)
        plt.bar(categories, values)
        plt.grid(visible=True, which='major', axis='y')


    def draw_bar_for_df(self, dataframe: pd.DataFrame, stacked: bool, is_horizontal: bool, colors: List = None):
        """
        Draws a bar for the provided dataframe.

        Parameters
        ----------
        categories : list
            categories
        values : list
            values
        subplot : int, optional
            if 0: draw a full plot, if > 0: number of subplot, by default 0
        bar_values : List, optional
            specifies values to plot for each bar, by default []
        """
        bar_kind = "bar"
        if is_horizontal:
            bar_kind = "barh"
        if colors:
            ax = dataframe.plot(kind=bar_kind, stacked=stacked, color=colors)
        else:
            ax = dataframe.plot(kind=bar_kind, stacked=stacked)


    def draw_histogram(self, data: list, bins: int, subplot: int = 0):
        """
        Draws a histogram.

        Parameters
        ----------
        data : list
            data
        bins : int
            bins
        subplot : int, optional
            if 0: draw a full plot, if > 0: number of subplot, by default 0
        """
        if subplot in (0, 1):  # either this plot has no sub-plots, or this is the first subplot
            plt.clf()
        if subplot > 0:
            plt.subplot(1, 2, subplot)
        plt.hist(data, bins=bins)


    def draw_histogram_cumulative(self, data: list, bins: int, title: str, xlabel: str, ylabel: str, y_axis_is_log: bool, filename: str, max_x_value: int = -1):
        """
        Draws a histogram with a cumulative line.

        Parameters
        ----------
        data : list
            data
        bins : int
            bins
        title: str
            plot title
        xlabel : str
            X label
        ylabel : str
            Y label
        y_axis_is_log : bool
            indicates whether the principal Y axis is logarithmic
        filename : str
            filename
        max_x_value : int
            maximum value for X axis
        """
        fig, axes = plt.subplots()  # pylint: disable=unused-variable
        axes.set_title(title)
        axes.set_xlabel(xlabel)
        axes.set_ylabel(ylabel)
        if max_x_value != -1:
            plt.xlim([0, max_x_value])
            max_value = max(data)
            bins = max(math.floor(max_value * bins / max_x_value), 1)
        axes.hist(data, bins=bins)
        if y_axis_is_log:
            axes.set_yscale("log")
        ax2 = axes.twinx()
        ax2.set_ylabel("Cumulative frequency")
        ax2.hist(data, cumulative=1, histtype='step', bins=100, color='tab:orange')
        plt.savefig(os.path.join(self.out_dir, filename), bbox_inches='tight', dpi=300)


    def set_plot_properties(self, title: str, xlabel: str, ylabel: str, max_xlim: int = None, max_ylim: int = None, filename: str = "", y_scale_is_log: bool = True, x_labels_vertical = False, x_ticks_size: int = -1, y_ticks_size: int = -1, x_ticks_rotation: int = -1):
        """
        Prepares basic properties for plots

        Parameters
        ----------
        title : str
            title of the histogram
        xlabel : str
            X label
        ylabel : str
            Y label
        max_xlim : int, optional
            maximum value for X axis, by default None
        max_ylim : int, optional
            maximum value for Y axis, by default None
        filename : str, optional
            if provided: name of the file where the plot will be stored, by default ""
        y_scale_is_log : bool, optional
            if yes: Y axis will be logarithmic, by default True
        x_labels_vertical: bool, optional
            if yes: X labels will be in vertical orientation, by default False
        x_ticks_size : int
            size of x axis ticks
        y_ticks_size : int
            size of y axis ticks
        x_ticks_rotation : in
            rotation of x labels
        """
        axes = plt.gca()
        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        if y_scale_is_log:
            plt.yscale("log")
        if max_xlim is not None:
            plt.xlim([None, max_xlim])
        if max_ylim is not None:
            plt.ylim([None, max_ylim])
        if x_labels_vertical:
            plt.xticks(rotation='vertical')
        if x_ticks_size != -1:
            for label in (axes.get_xticklabels()):
                label.set_fontsize(x_ticks_size)
        if y_ticks_size != -1:
            for label in (axes.get_yticklabels()):
                label.set_fontsize(y_ticks_size)
        if x_ticks_rotation != -1:
            axes.set_xticklabels(axes.get_xticklabels(), rotation = x_ticks_rotation)
        if filename != "":
            plt.savefig(os.path.join(self.out_dir, filename), bbox_inches='tight', dpi=300)


    def draw_heatmap(self, dataframe: DataFrame, add_annotations: bool = True, clear_fig: bool = True, y_label_size: int = -1, colors: str = "YlGnBu"):
        """
        Draws a heatmap.

        Parameters
        ----------
        dataframe : DataFrame
            dataframe to display
        filename : str
            name of the file where the plot will be stored
        add_annotations : bool, optional
            if yes: add values on the graph, by default True
        clear_fig : bool, optional
            if yes: previous figure will be cleared, by default True
        """
        if clear_fig:
            plt.clf()
        heatmap = sb.heatmap(dataframe, annot=add_annotations, fmt=',', linewidths=.4, cmap=colors, xticklabels=True, yticklabels=True)
        if y_label_size != -1:
            heatmap.set_yticklabels(heatmap.get_ymajorticklabels(), fontsize = y_label_size)
        #plt.savefig(os.path.join(self.out_dir, filename), bbox_inches='tight', dpi=300)


    def _draw_upset(self, upset, filename: str, clear_fig: bool = True):
        """
        Draws an upset plot.

        Parameters
        ----------
        upset : UpSet
            UpSet data
        filename : str
            name of the file where the plot will be stored
        clear_fig : bool, optional
            if yes: previous figure will be cleared, by default True
        """
        if clear_fig:
            plt.clf()
        upset.style_subsets(present="1a", edgecolor="darkorchid", label="contains 1a")
        upset.style_subsets(present="1b", facecolor="green", label="contains 1b")
        upset.plot()
        plt.savefig(os.path.join(self.out_dir, filename), dpi=300)
