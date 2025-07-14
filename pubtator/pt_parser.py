"""
Parses a PubTator XML file.
Retrieves relevant documents (annotated with relevant species).
For each relevant document, looks for traits.
For each detected trait, if applicable, creates (species, gene, trait) associations.
The extracted information is stored in the database.
"""

import xml.etree.ElementTree as ET          # XML parsing
import time                                 # for performance measures
import enum                                 # ... for enums!
from collections import Counter             # to retrieve top highest values in a dictionary
import os
from timeit import default_timer as timer   # to measure execution time
from typing import List, Dict               # for type hints

from pubtator.pt_elements import PubTatorDocument, PubTatorParagraph
from tools import tools
from tools.database_handler import DatabaseHandler
from tools.spacy_text_analyzer import SpacyTextAnalyzer


class AssociationType(enum.Enum):
    """ Represents all possible association types.
    """
    CASE_1A = 1  # gene and trait in the same sentence, species is associated to the gene via tax_id
    CASE_1B = 2  # gene, trait and species in the same sentence
    CASE_1C = 3  # gene and trait in the same sentence, species in the same paragraph
    CASE_1D = 4  # gene and trait are in the same sentence, species is in the document (title or abstract)
    CASE_2A = 5  # gene and trait in the same paragraph, species is associated to the gene via tax_id
    CASE_2BA = 6  # gene and trait are in the same paragraph, species is in the same sentence as trait
    CASE_2BB = 7  # gene and trait are in the same paragraph, species is in the same sentence as gene
    CASE_2C = 8  # gene, trait and species are in the same paragraph
    CASE_2D = 9  # gene and trait are in the same paragraph, species is in the document (title or abstract)


class PubTatorParser():
    """
    Parses a PubTator XML file.
    Retrieves relevant documents (annotated with relevant species).
    For each relevant document, looks for traits.
    For each detected trait, if applicable, creates (species, gene, trait) associations.
    The extracted information is stored in the database.
    """

    def __init__(self, db_handler: DatabaseHandler, species_dict: Dict, trait_dict: Dict, species_synonyms_black_list_file_name: str, gene_synonyms_black_list_file_name: str, out_dir: str,  tmp_dir: str, verbose: bool = True):
        """
        Constructor.

        Parameters
        ----------
        db_handler : DatabaseHandler
            allows to perform database-related operations
        species_dict : Dict
            dictionary of relevant species, in form 'id : [synonyms]'
        trait_dict : Dict
            trait synonyms dictionary, in form "id : synonym1 | synonym2 | ..."
        species_synonyms_black_list_file_name : str
            name of the file containing the black list of species synonyms
        gene_synonyms_black_list_file_name : str
            name of the file containing the black list of gene synonyms
        out_dir : str
            directory for the log file
        tmp_dir : str
            directory for temporary spaCy files
        verbose : bool, optional
            indicates whether all messages should be printed
        """
        tools.print_info_message("INITIALIZING TEXT MINING...", 0)

        # text mining related
        self.db_handler = db_handler
        self.species_dict = species_dict
        self.species_synonyms_black_list = self._parse_synonyms_black_list_file(species_synonyms_black_list_file_name)
        self.gene_synonyms_black_list = self._parse_synonyms_black_list_file(gene_synonyms_black_list_file_name)
        self.spacy_analyzer = SpacyTextAnalyzer(trait_dict, tmp_dir)
        self.out_dir = out_dir
        self.verbose = verbose

        # statistics
        self.all_documents_cnt = 0
        self.retained_documents_cnt = 0
        self.failed_documents = 0
        self.relevant_species_in_documents_distr = {}

        # working attributes
        self.print_graphs = False
        self.start_parse_time = 0
        self.log_file = None

        
    def parse_annotations(self, pubtator_file_name_pattern: str, start_doc_idx: int, end_doc_idx: int):
        """
        PubTator annotations are provided in XML format, in several XML files.
        One XML file contains a concatenation of "document sets".
        One document set is composed of 100 annotated documents.
        Each document is encoded on one line.
        This function reads a set of XML files, with indices starting at 'start_doc_idx' and ending
        at 'end_doc_idx', retrieves lines corresponding to documents
        and calls _parse_document in order to extract annotated PubTator documents.

        Parameters
        ----------
        pubtator_file_name_pattern : str
            the name of a file with PubTator annotations
        start_doc_idx : int
            starting index of PubTator documents
        end_doc_idx : int
            ending index of PubTator documents
        """
        
        self.start_parse_time = timer()
        tools.print_info_message(f"Parsing PubTator documents, with indices between '{start_doc_idx}' and '{end_doc_idx}'...")
        for idx in range(start_doc_idx, end_doc_idx+1):
            pubtator_file_name = pubtator_file_name_pattern.replace('XXXXX', str(idx))
            tools.print_info_message(f"Reading PubTator annotations file {pubtator_file_name}...", 2)
            with open(pubtator_file_name, encoding='utf-8') as pubtator_file:
                with open(os.path.join(self.out_dir, "pubtator_parser_log.txt"), "w+", encoding='utf-8') as self.log_file:
                    line = pubtator_file.readline()
                    self._write_log_line("Parsing started.")
                    while line:
                        if '<document>' in line:
                            self._parse_document(line)
                        try:
                            line = pubtator_file.readline()
                        except MemoryError as exception:
                            tools.print_exception_message(f"Memory error occurred during document parsing: {exception}. Switching to the next document.")
                    self._write_log_line("Parsing ended.")
        self._print_statistics()
        self._print_top_species_and_singletons()
        if self.retained_documents_cnt == 0:
            tools.print_error_message(f"No documents retained from the {self.all_documents_cnt} analyzed documents!")


    def _parse_synonyms_black_list_file(self, synonyms_black_list_file_name: str):
        """
        Reads the file with the black list of synonyms and adds them to a list.

        Parameters
        ----------
        synonyms_black_list_file_name : str
            name of the file containing a black list of synonyms, one synonym per line

        Returns
        -------
        list
            list of black-listed synonyms
        """
        synonyms_black_list = []
        with open(synonyms_black_list_file_name, encoding="utf-8") as synonyms_black_list_file:
            line = synonyms_black_list_file.readline()
            while line:
                synonyms_black_list.append(line.strip().lower())
                line = synonyms_black_list_file.readline()
        return synonyms_black_list


    def _parse_document(self, content: str):
        """
        Parses the provided content, representing one PubTator document.

        Parameters
        ----------
        content : str
            one line from PubTator XML file, representing a document
        """

        try:
            self.all_documents_cnt += 1
            document_root = ET.fromstring(content)
            new_document = PubTatorDocument(document_root, self.species_dict, self.species_synonyms_black_list, self.gene_synonyms_black_list)

            if new_document.is_relevant():
                for species_id in new_document.relevant_species_ids:
                    if species_id in self.species_dict.keys():
                        self.relevant_species_in_documents_distr.setdefault(species_id, 0)
                        self.relevant_species_in_documents_distr[species_id] += 1

                self._search_traits_in_document(new_document)

            if self.all_documents_cnt % 1000 == 0:
                self._print_statistics()

        except Exception as exception:
            self.failed_documents += 1
            tools.print_exception_message(f"Error occurred during document parsing: {exception}. Total failed documents: {self.failed_documents}")


    def _search_traits_in_document(self, document: PubTatorDocument):
        """
        Retrieves traits in the provided document.
        If traits are found, the document is added to the database.

        Parameters
        ----------
        document : PubTatorDocument
            document to analyze
        """

        try:
            doc_id = -1
            doc_handle_start_time = time.time()
            for par_idx, paragraph in enumerate(document.paragraphs):
                # as we want traits and genes to occur within the same paragraph, we don't process paragraphs without gene annotations
                if len(paragraph.gene_annotations) > 0:
                    matches = self.spacy_analyzer.extract_term_matches_from_text(paragraph.text)
                    if matches:
                        if doc_id == -1:
                            doc_id, par_ids = self.db_handler.add_pubtator_document(document)  # the document is only added to the database if relevant trait matches are found
                            self.retained_documents_cnt += 1
                        for match in matches:
                            try:
                                annotation_id = self.db_handler.add_trait_annotation(par_ids[par_idx], match['id'], match['start'], match['length'], match['synonym'])
                                match['ann_id'] = annotation_id
                            except Exception as exception:
                                tools.print_exception_message(f"Error occurred while adding trait annotation '{match['id']}' with synonym '{match['synonym']}': {exception}")
                        self._retrieve_species_gene_trait_associations(document, doc_id, paragraph, par_ids[par_idx], matches)

            doc_handle_time = time.time() - doc_handle_start_time
            if self.verbose:
                tools.print_info_message(f"=== doc handle time: {doc_handle_time:.3f}s ({doc_handle_time/len(document.paragraphs):.3f}s per paragraph) ===")
        except Exception as exception:
            tools.print_exception_message(f"Error occurred while searching for trait annotations: {exception}")

    
    def _retrieve_species_gene_trait_associations(self, document: PubTatorDocument, doc_id: int, paragraph: PubTatorParagraph, par_id: int, matches: List):
        """
        Extracts (species, gene, trait) associations from the provided list of matches.
        If all three entities are close to each other, the score of their association is high.
        Genes are only considered if they are situated in the same paragraph as the trait.
        Species are both considered on the paragraph and document level.
        Species are only taken into account if their tax id matches the tax id of the gene in the association.

        Parameters
        ----------
        document : PubTatorDocument
            current PubTator document
        doc_id : int
            id of the document in the database
        paragraph : PubTatorParagraph
            current PubTator paragraph
        par_id : int
            paragraph id in the database
        matches : List
            list of trait matches, containing "synonym-start-length-ann_id" information
        """

        try:
            # FIRST LOOP: trait matches
            for match in matches:
                trait_synonym = match['synonym']
                trait_id = match['id']
                trait_ann_id = match["ann_id"]
                match_pos = match['start']
                paragraph_score = self._compute_paragraph_score(paragraph, matches)
                self._write_log_line(f"...{paragraph.text[max(0,match_pos-50) : match_pos]}____{paragraph.text[match_pos : match_pos + len(match)]}____{paragraph.text[match_pos + len(match) : min(match_pos+len(match)+50, len(paragraph.text)-1)]}... (synonym: {match})")
                # SECOND LOOP: genes
                for gene_annotation in paragraph.gene_annotations:
                    (sent_start, sent_end) = self._retrieve_sentence_boundaries_at_position_from_text(paragraph.text, match_pos)
                    spec_ann_ids = set()  # stores ids of species annotations added so far to the couple (trait, gene) (to avoid to add the same annotations in "d" cases)
                    for ncbi_gene_id in gene_annotation.ids:
                        tax_id = self.db_handler.get_ncbi_gene_tax_id(ncbi_gene_id)
                        gene_id = self.db_handler.get_gene_id_from_ncbi_id(ncbi_gene_id)
                        gene_ann_id = self.db_handler.get_gene_annotation_id(par_id, ncbi_gene_id, gene_annotation.offset)
                        if sent_start < gene_annotation.offset < sent_end:  # trait and gene are in the same sentence
                            if tax_id != -1 and str(tax_id) in self.species_dict.keys():  # case 1a: trait and gene in the same sentence, species is associated to the gene via tax_id
                                self.db_handler.add_association(doc_id, par_id, tax_id, None, gene_id, gene_ann_id, trait_id, trait_ann_id, trait_synonym, AssociationType.CASE_1A.value, 60)
                            # THIRD LOOP: species
                            for spec_annotation in paragraph.species_annotations:
                                for spec_id in spec_annotation.ids:
                                    tax_id_relevant = tax_id == int(spec_id)
                                    if tax_id_relevant:
                                        spec_ann_id = self.db_handler.get_spec_annotation_id(par_id, spec_id, spec_annotation.offset)
                                        spec_ann_ids.add(spec_ann_id)
                                        if sent_start < spec_annotation.offset < sent_end:  # case 1b: trait, gene and species in the same sentence
                                            self.db_handler.add_association(doc_id, par_id, spec_id, spec_ann_id, gene_id, gene_ann_id, trait_id, trait_ann_id, trait_synonym, AssociationType.CASE_1B.value, 100)
                                            spec_ann_ids.add(spec_ann_id)
                                        else:  # case 1c: trait and gene in the same sentence, species in the same paragraph
                                            score = round(80*paragraph_score['species'])
                                            if score > 0:
                                                self.db_handler.add_association(doc_id, par_id, spec_id, spec_ann_id, gene_id, gene_ann_id, trait_id, trait_ann_id, trait_synonym, AssociationType.CASE_1C.value, score)
                                                spec_ann_ids.add(spec_ann_id)
                            # case 1d: gene and trait are in the same sentence, species is in the document (title or abstract)
                            self._retrieve_d_cases(document.relevant_species_annotations_in_title, document.relevant_species_ids, spec_ann_ids, tax_id, doc_id, par_id, gene_id, gene_ann_id, trait_id, trait_ann_id, trait_synonym, AssociationType.CASE_1D.value, 2, 50)
                            self._retrieve_d_cases(document.relevant_species_annotations_in_abstract, document.relevant_species_ids, spec_ann_ids, tax_id, doc_id, par_id, gene_id, gene_ann_id, trait_id, trait_ann_id, trait_synonym, AssociationType.CASE_1D.value, 2, 40)
                        else:  # trait and gene are in the same paragraph
                            if tax_id != -1 and str(tax_id) in self.species_dict.keys() and len(paragraph.species_annotations) == 0:
                                # case 2a: trait and gene in the same paragraph, species is associated to the gene via tax_id
                                self.db_handler.add_association(doc_id, par_id, tax_id, None, gene_id, gene_ann_id, trait_id, trait_ann_id, trait_synonym, AssociationType.CASE_2A.value, 40)
                            # THIRD LOOP: species
                            for spec_annotation in paragraph.species_annotations:
                                for spec_id in spec_annotation.ids:
                                    tax_id_relevant = tax_id == int(spec_id)
                                    if tax_id_relevant:
                                        spec_ann_id = self.db_handler.get_spec_annotation_id(par_id, spec_id, spec_annotation.offset)
                                        association_arguments = (doc_id, par_id, spec_id, spec_ann_id, gene_id, gene_ann_id, trait_id, trait_ann_id, trait_synonym)
                                        if sent_start < spec_annotation.offset < sent_end:  # case 2.ba: gene and trait are in the same paragraph, species is in the same sentence as trait
                                            score = round(60*paragraph_score['genes'])
                                            if score > 0:
                                                self.db_handler.add_association(*association_arguments, AssociationType.CASE_2BA.value, score)
                                                spec_ann_ids.add(spec_ann_id)
                                        else:
                                            (sp_sent_start, sp_sent_end) = self._retrieve_sentence_boundaries_at_position_from_text(paragraph.text, spec_annotation.offset)
                                            (g_sent_start, g_sent_end) = self._retrieve_sentence_boundaries_at_position_from_text(paragraph.text, gene_annotation.offset)
                                            if sp_sent_start == g_sent_start and sp_sent_end == g_sent_end:  # case 2.bb: gene and trait are in the same paragraph, species is in the same sentence as gene
                                                score = round(60*paragraph_score['traits'])
                                                if score > 0:
                                                    self.db_handler.add_association(*association_arguments, AssociationType.CASE_2BB.value, score)
                                                    spec_ann_ids.add(spec_ann_id)
                                            else:  # case 2c: gene, trait and species are in the same paragraph
                                                score = round(50*paragraph_score['total'])
                                                if score > 0:
                                                    self.db_handler.add_association(*association_arguments, AssociationType.CASE_2C.value, score)
                                                    spec_ann_ids.add(spec_ann_id)
                            # case 2d: gene and trait are in the same paragraph, species is in the document (title or abstract)
                            self._retrieve_d_cases(document.relevant_species_annotations_in_title, document.relevant_species_ids, spec_ann_ids, tax_id, doc_id, par_id, gene_id, gene_ann_id, trait_id, trait_ann_id, trait_synonym, AssociationType.CASE_2D.value, 1, 30)
                            self._retrieve_d_cases(document.relevant_species_annotations_in_abstract, document.relevant_species_ids, spec_ann_ids, tax_id, doc_id, par_id, gene_id, gene_ann_id, trait_id, trait_ann_id, trait_synonym, AssociationType.CASE_2D.value, 1, 20)
        except Exception as exception:
            tools.print_exception_message(f"Error occurred while retrieving SGT associations: {exception}")

    
    def _compute_paragraph_score(self, paragraph: PubTatorParagraph, matches: List):
        """
        Computes the paragraph score: the total score and the score for traits, genes and species.
        The paragraph score depends on the number of distinct items it has of the same type: the more distinct items, the lower the score.
        If a paragraph has several annotations for the same trait/gene/species id, they are not considered as distinct.
        The value of the score is between 0 and 1.

        Parameters
        ----------
        paragraph : PubTatorParagraph
            [description]
        matches : List
            [description]

        Returns
        -------
        [type]
            [description]
        """
        # count the number of unique trait, gene and species ids in the provided paragraph, using sets to remove duplicates
        unique_trait_ids = {match['id'] for match in matches}
        unique_gene_ids = set([item for sublist in [gene_ann.ids for gene_ann in paragraph.gene_annotations] for item in sublist])
        unique_species_ids = set([item for sublist in [spec_ann.ids for spec_ann in paragraph.species_annotations] for item in sublist])

        # count the number of distinct elements per type - 1 (ex: if the paragraph has annotations for 2 different genes, there is one "problematic" element)
        distinct_traits_cnt = max(1, len(unique_trait_ids)) - 1
        distinct_genes_cnt = max(1, len(unique_gene_ids)) - 1
        distinct_species_cnt = max(1, len(unique_species_ids)) - 1
        distinct_total_dupl_count = distinct_traits_cnt + distinct_genes_cnt + distinct_species_cnt

        # computation of the result
        result = {}
        result['total'] = max(0, (10 - distinct_total_dupl_count)) / 10
        result['traits'] = max(0, (10 - distinct_traits_cnt*2)) / 10
        result['genes'] = max(0, (10 - distinct_genes_cnt*2)) / 10
        result['species'] = max(0, (10 - distinct_species_cnt*2)) / 10
        return result

    
    def _retrieve_d_cases(self, spec_annotations: List, doc_spec_ids: Dict, spec_ann_ids: List, tax_id: int, doc_id: int, par_id: int, gene_id: int, gene_ann_id: int, trait_id: int, trait_ann_id: int, trait_synonym: str,
                          assoc_type: int, multiplier: int, max_score: int):
        """
        Retrieves "d" cases (where trait and gene are in the same sentence or paragraph and the species is in the same document, in title or abstract).
        If the list of provided annotations contains several annotations for the same species, only the first annotation is added (but the amount of
        annotations of this species is taken into account in the score).
        Only annotations for species with the same tax id as the tax id associated to the gene are taken into account.

        Parameters
        ----------
        spec_annotations : List
            list of annotations to create triples with
        doc_spec_ids : Dict
            dictionary of species ids present in the current document, associated to the number of their occurrences
        spec_ann_ids : List
            list of species annotations already added for the couple (trait/gene), to avoid adding them twice
        tax_id : int
            NCBI species tax_id associated to the gene
        doc_id : int
            id of the document in the database
        par_id : int
            id of the the paragraph containing the gene and the trait
        gene_id : int
            NCBI id of the gene
        gene_ann_id : int
            id of the gene annotation in the database
        trait_id : int
            id of the trait
        trait_ann_id : int
            id of the trait annotation in the database
        trait_synonym : str
            trait synonym
        assoc_type : int
            association type (1d or 2d)
        multiplier : int
            used to compute the evidence score with the formula 'min(multiplier*nb_of_spec_occurrences_in_the_document, max_score)'
        max_score : int
            used to compute the evidence score with the formula 'min(multiplier*nb_of_spec_occurrences_in_the_document, max_score)'
        """
        added_spec_ids = []
        for spec_annotation in spec_annotations:
            for spec_id in spec_annotation.ids:
                if tax_id == int(spec_id):  # the species id corresponds to the provided tax id
                    try:
                        ann_par_id = self.db_handler.get_paragraph_id(doc_id, spec_annotation.paragraph.text)
                        spec_ann_id = self.db_handler.get_spec_annotation_id(ann_par_id, spec_id, spec_annotation.offset)    
                        if spec_ann_id not in spec_ann_ids and spec_id not in added_spec_ids:
                            self.db_handler.add_association(doc_id, par_id, spec_id, spec_ann_id, gene_id, gene_ann_id, trait_id, trait_ann_id, trait_synonym, assoc_type, min(multiplier*doc_spec_ids[spec_id], max_score))
                            added_spec_ids.append(spec_id)
                    except Exception as exception:
                        tools.print_exception_message(f"Error occurred while retrieving d cases: '{exception}'")

    def _retrieve_sentence_boundaries_at_position_from_text(self, text: str, position: int):
        """
        Extracts the boundaries (start, length) of the sentence situated at the given
        position of the given text.

        Parameters
        ----------
        text : str
            text to analyze
        position : int
            position of interest

        Returns
        -------
        (int, int)
            boundaries of the sentence (start, length)
        """
        sentences = self.spacy_analyzer.extract_sentences(text)
        current_pos = 0
        sentence = None
        sent_start = -1
        for sentence in sentences:
            if current_pos + len(sentence.text) > position:
                break
            else:
                current_pos += len(sentence.text)
        if sentence:
            sent_start = text.find(sentence.text)
        return sent_start, sent_start + len(sentence.text)


    def _print_statistics(self):
        """
        Diplays the number of read documents, relevant documents, relevant species and parsing time per document.
        """
        assert len(self.species_dict) > 0, "species dictionary is empty"
        retained_documents_perc = self.retained_documents_cnt * 100 / self.all_documents_cnt if self.all_documents_cnt > 0 else 0
        elapsed_time = timer() - self.start_parse_time
        time_per_document = ( elapsed_time / self.retained_documents_cnt ) if self.retained_documents_cnt > 0 else 0
        tools.print_info_message("===== IMPORT STATS =============", 2)
        tools.print_info_message(f"Documents read     : {self.all_documents_cnt:,}", 2)
        tools.print_info_message(f"Retained documents : {self.retained_documents_cnt:,} ({retained_documents_perc:.2f}%)", 2)
        tools.print_info_message(f"Time per document  : {time_per_document:5.4f}", 2)
        tools.print_info_message("================================", 2)


    def _print_top_species_and_singletons(self):
        """
        Displays the 20 most common relevant species extracted so far.
        """
        tools.print_info_message("Top species + number of documents they are found in:", 2)
        top_relevant_species = dict(Counter(self.relevant_species_in_documents_distr).most_common(20))
        for key, value in top_relevant_species.items():
            tools.print_info_message(f"{value:,} : {self.species_dict[key][:60]}...", 3)

        relevant_singletons = len([item for item in self.relevant_species_in_documents_distr.values() if item == 1])
        tools.print_info_message(f"Singletons: {relevant_singletons:,} out of {len(self.relevant_species_in_documents_distr):,}", 2)
        tools.print_info_message("================================", 2)


    def _write_log_line(self, line: str):
        """
        Writes a line in the log file.

        Parameters
        ----------
        line : str
            text to write
        """
        self.log_file.write(f"{tools.time_now()} -- {line}\n")
        self.log_file.flush()
