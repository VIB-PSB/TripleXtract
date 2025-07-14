"""
Interface allowing to handle database-related operations.
"""

import csv
import collections
import operator
from typing import Counter, Dict, List

from clint.textui import progress
import mysql.connector

from pubtator.pt_elements import PubTatorDocument, PubTatorParagraph, PubTatorAnnotation
from tools import tools

# pylint: disable=missing-function-docstring,too-many-lines


class DatabaseHandler():
    """
    Interface allowing to handle database-related operations.
    """

    def __init__(self, db_name: str, db_host: str, db_user: str, db_password: str, verbose: bool = False):
        self.verbose = verbose
        self.db_name = db_name
        self.db_host = db_host
        self.db_user = db_user
        self.db_password = db_password
        self.database = self.connect_to_db()
        self.cursor = self.database.cursor()


    def connect_to_db(self):
        """
        Connects to the database.

        Returns
        -------
        MySQLConnection
            [description]

        Raises
        ------
        Exception
            raized in case of connection failure
        """
        tools.print_info_message(f"CONNECTING TO THE DATABASE {self.db_name}...", 0)
        database = mysql.connector.connect(host=self.db_host,user=self.db_user,password=self.db_password,database=self.db_name)
        if database:
            tools.print_info_message("Connected.")
        else:
            raise Exception("Failure during database connection.")
        return database


    def wake_up_connection(self):
        """
        Checks whether the database is still connected and if not, reconnects.
        """
        if not self.database.is_connected():
            tools.print_warning_message("Lost connection to the database! Reconnecting...")
            self.database.reconnect()
        self.database.ping(reconnect=True)
        if not self.database.is_connected():
            tools.print_error_message("Could not establish connection to the database.")


    def purge_database(self, purge_species_synonyms = False, purge_gene_synonyms = False, purge_to = False, purge_ncbi = False, purge_plaza_genes = False, purge_plaza_orthology = False):
        """
        Removes the content of the database.

        Parameters
        ----------
        purge_species_synonyms : bool, optional
            remove or not the table "species_synonym", by default False
        purge_gene_synonyms : bool, optional
            remove or not the table "gene_synonym", by default False
        purge_to : bool, optional
            remove or not the table "trait_synonym", by default False
        purge_ncbi : bool, optional
            remove or not the table "ncbi_gene2accession", by default False
        purge_plaza_genes : bool, optional
            remove or not the tables "plaza_gene_synonym" and "plaza_species_id", by default False
        purge_plaza_orthology : bool, optional
            remove or not the table "plaza_orthology", by default False
        """
        tools.print_info_message("PURGING THE DATABASE...", 0)
        self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        self.cursor.execute("TRUNCATE association")
        self.cursor.execute("TRUNCATE author")
        self.cursor.execute("TRUNCATE paragraph")
        self.cursor.execute("TRUNCATE document")
        self.cursor.execute("TRUNCATE tm_species_annotation")
        self.cursor.execute("TRUNCATE tm_gene_annotation")
        self.cursor.execute("TRUNCATE tm_trait_annotation")
        self.cursor.execute("TRUNCATE tm_evidence")
        if purge_species_synonyms:
            self.cursor.execute("TRUNCATE species_synonym")  # this table is normally kept as the import is done once for all the NCBI plant taxonomy
        if purge_gene_synonyms:
            self.cursor.execute("TRUNCATE gene_synonym")  # this table is normally kept as the import is done once for all the NCBI genes (takes +/- 30 minutes)
        if purge_to:
            self.cursor.execute("TRUNCATE trait_synonym")
        if purge_ncbi:
            self.cursor.execute("TRUNCATE ncbi_gene2accession")
        if purge_plaza_genes:
            self.cursor.execute("TRUNCATE plaza_gene_synonym")
            self.cursor.execute("TRUNCATE plaza_species_id")
        if purge_plaza_orthology:
            self.cursor.execute("TRUNCATE plaza_orthology")
        self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        self.database.commit()
        tools.print_info_message("Database purged.")


    def import_species_dict(self, species_dict: dict):
        self.wake_up_connection()
        tools.print_info_message("Importing species dictionary to the database...")
        sql = "INSERT INTO species_synonym (id, ncbi_synonyms) VALUES (%s, %s)"
        values = []
        for key in species_dict.keys():
            values.append((key, species_dict[key]))
        self.cursor.executemany(sql, values)
        self.database.commit()
        tools.print_info_message("Done.", 2)


    def export_species_dict(self):
        self.cursor.execute("SELECT id, ncbi_synonyms FROM species_synonym")
        species_synonyms = self.cursor.fetchall()
        result = {}
        for value in species_synonyms:
            result[str(value[0])] = value[1]
        return result


    def import_gene_dict(self, gene_dict: dict):
        self.wake_up_connection()
        tools.print_info_message("Importing gene dictionary in the database...")
        sql = "INSERT INTO gene_synonym (ncbi_id, ncbi_synonyms, symbol, tax_id, locus_tag, db_xref, source) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        values = []
        item_cnt = 0
        for key in gene_dict.keys():
            values.append((key, gene_dict[key]["synonyms"], gene_dict[key]["symbol"], gene_dict[key]["tax_id"], gene_dict[key]["locus_tag"], gene_dict[key]["db_xref"], "NCBI"))
            item_cnt += 1
            if item_cnt % 10_000 == 0:
                self.cursor.executemany(sql, values)
                self.database.commit()
                values = []
                if self.verbose and item_cnt % 5_000_000 == 0:
                    tools.print_info_message(f"Imported {item_cnt:,} genes so far...", 2)
        self.cursor.executemany(sql, values)
        self.database.commit()
        tools.print_info_message(f"Imported {item_cnt:,} genes.", 2)


    def import_ontology_dict(self, ontology_dict: dict, ontology_name: str):
        self.wake_up_connection()
        tools.print_info_message(f"Importing {ontology_name} ontology to the database...", 0)
        sql = "INSERT INTO trait_synonym (id, synonyms) VALUES (%s, %s)"
        values = []
        for key in ontology_dict.keys():
            values.append((key, ontology_dict[key]))
        self.cursor.executemany(sql, values)
        self.database.commit()


    def get_trait_synonyms(self):
        self.cursor.execute("SELECT id, synonyms FROM trait_synonym")
        data = self.cursor.fetchall()
        return data


    # @tools.timeit
    def add_pubtator_document(self, document: PubTatorDocument):
        title = (document.title[:995] + "...") if len(document.title) >= 1000 else document.title # some titles are longer than the length accepted by the database
        journal = (document.journal[:95] + "...") if len(document.journal) >= 1000 else document.journal # some journals are longer than the length accepted by the database
        sql = "INSERT INTO document (title, doi, pubmed_id, pmc_id, sici, publisher_id, year, journal, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        values = (title, document.doi, document.pubmed_id, document.pmc_id, document.sici, document.publisher_id, document.year, journal, document.volume[:50])
        self.cursor.execute(sql, values)
        self.database.commit()

        doc_id = self.cursor.lastrowid
        self._add_authors_to_document(document.authors, doc_id)
        par_ids = self._add_paragraphs_to_document(document.paragraphs, doc_id)
        if self.verbose:
            tools.print_info_message(f"Document {document.pubmed_id} inserted -- {len(document.paragraphs)} paragraphs.", 3)
        return doc_id, par_ids


    def add_trait_annotation(self, par_id: int, trait_id: int, offset: int, trait_length: int, trait_synonym: str):
        result = -1
        sql = "INSERT INTO tm_trait_annotation (par_id, trait_id, offset, length, text) VALUES (%s, %s, %s, %s, %s)"
        values = (par_id, trait_id, offset, trait_length, trait_synonym)
        self.cursor.execute(sql, values)
        self.database.commit()
        result =  self.cursor.lastrowid
        return result


    def get_paragraph_id(self, doc_id: int, text: str):
        sql = 'SELECT p.id FROM paragraph p WHERE p.doc_id=%s AND p.text=%s'
        values = (doc_id, text)
        self.cursor.execute(sql, values)
        par_ids = self.cursor.fetchall()
        if len(par_ids) > 1:
            tools.print_warning_message(f'There are more than one paragraph with the text "{text}" in document {doc_id}')
        return par_ids[0][0]


    def get_spec_annotation_id(self, par_id: int, spec_id: int, offset: int):
        self.cursor.execute(f"SELECT id FROM tm_species_annotation WHERE par_id={par_id} AND spec_id={spec_id} AND offset={offset}")
        spec_ids = self.cursor.fetchall()
        if len(spec_ids) > 1:
            tools.print_warning_message(f"There are more than one species annotation with the paragraph id {par_id}, species id {spec_id} and offset {offset}")
        return spec_ids[0][0]


    def get_gene_annotation_id(self, par_id: int, gene_id: int, offset: int):
        self.cursor.execute(f"SELECT id FROM tm_gene_annotation WHERE par_id={par_id} AND gene_id={gene_id} AND offset={offset}")
        gene_ids = self.cursor.fetchall()
        if len(gene_ids) > 1:
            tools.print_warning_message(f"There are more than one gene annotation with the paragraph id {par_id}, gene id {gene_id} and offset {offset}")
        return gene_ids[0][0]


    def get_ncbi_gene_tax_id(self, ncbi_gene_id: int):
        result = -1
        self.cursor.execute(f'SELECT tax_id FROM gene_synonym WHERE ncbi_id={ncbi_gene_id} AND source LIKE "%NCBI%"')
        tax_ids = self.cursor.fetchall()
        if tax_ids:
            result = tax_ids[0][0]
        return result


    def get_gene_id_from_ncbi_id(self, ncbi_gene_id: int):
        result = -1
        self.cursor.execute(f'SELECT id FROM gene_synonym WHERE ncbi_id={ncbi_gene_id}')
        data = self.cursor.fetchall()
        if data:
            result = data[0][0]
        return result


    def get_species_name_from_tax_id(self, tax_id: int):
        result = -1
        self.cursor.execute(f'SELECT ncbi_synonyms FROM species_synonym WHERE id={tax_id}')
        data = self.cursor.fetchall()
        if data:
            result = data[0][0]
        return result


    def add_association(self, doc_id, par_id, spec_id, spec_ann_id, gene_id, gene_ann_id, trait_id, trait_ann_id, trait_synonym, type_id, score):
        try:
            spec_id = self._patch_spec_id(spec_id)
            assoc_id = self._get_assoc_id(spec_id, gene_id, trait_id)

            sql = "INSERT INTO tm_evidence (assoc_id, doc_id, par_id, spec_ann_id, gene_ann_id, trait_ann_id, trait_synonym, type_id, score) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (assoc_id, doc_id, par_id, spec_ann_id, gene_ann_id, trait_ann_id, trait_synonym, type_id, score)
            self.cursor.execute(sql, values)
            self.database.commit()
        except Exception as exception:
            tools.print_exception_message(f"Exception occurred while adding association: {exception}")
        
        
    def _patch_spec_id(self, spec_id: int):
        """
        PATCH: for O. sativa, in order to match with the tax id of its genes, we replace the species id by 39947

        Parameters
        ----------
        spec_id : int
            a species id

        Returns
        -------
        int
            the patched spec id
        """
        if spec_id == 4530:
            spec_id = 39947
        return spec_id
        
        
    def _get_assoc_id(self, spec_id, gene_id, trait_id, retry_cnt = 1):
        result = -1
        try:
            if gene_id is not None:
                self.cursor.execute(f'SELECT id FROM association WHERE spec_id={spec_id} AND gene_id={gene_id} AND trait_id="{trait_id}"')
            else:
                self.cursor.execute(f'SELECT id FROM association WHERE spec_id={spec_id} AND gene_id IS NULL AND trait_id="{trait_id}"')
            assoc_id = self.cursor.fetchall()
            if assoc_id:
                result = assoc_id[0][0]
            else:
                sql = "INSERT INTO association (spec_id, gene_id, trait_id) VALUES (%s, %s, %s)"
                values = (spec_id, gene_id, trait_id)
                self.cursor.execute(sql, values)
                self.database.commit()
                result = self.cursor.lastrowid
        except Exception as exception:
            tools.print_exception_message(f"Exception occurred during assoc id retrieval: {exception}")
            retry_cnt += 1
        finally:
            if result != -1 or retry_cnt == 5:  # 'result != -1' means the result could be computed, retry_cnt = 5 means all 5 attempts failed
                if result != -1 and retry_cnt > 1:  # at least one attempt failed
                    tools.print_info_message(f"Successfully retrieved association id after {retry_cnt} attempts.")
                return result  # pylint: disable=lost-exception
            else:
                self.database.rollback()
                tools.print_info_message(f"Attempt number {retry_cnt}...")
                return self._get_assoc_id(spec_id, gene_id, trait_id, retry_cnt+1)

    
    def get_gene_ids_for_a_thaliana(self):
        """
        Retrieves tuples (locus_tag, gene_id) for A. thaliana and converts them to a dictionary.

        Returns
        -------
        _type_
            _description_
        """
        self.cursor.execute('SELECT locus_tag, id FROM gene_synonym WHERE tax_id=3702 AND locus_tag <> "-"')
        data = self.cursor.fetchall()
        return dict(data)


    def get_tm_associations_for_tax_id(self, tax_id: int):
        self.cursor.execute("SELECT DISTINCT trait_id, gs.plaza_id AS gene_id "
                            "FROM association a "
                            "INNER JOIN tm_evidence te ON te.assoc_id = a.id "
                            "INNER JOIN gene_synonym gs ON gs.id = a.gene_id "
                            f"WHERE a.spec_id = {tax_id}")
        data = self.cursor.fetchall()
        return list(map(list, zip(*data)))
    
    
    def add_document(self, title: str, doi: str, pubmed_id: int, pmc_id: int, sici: str, publisher_id: str, year: int, journal: str, volume: str, author_first_name: str, author_last_name: str):
        result = -1
        self.cursor.execute(f'SELECT id FROM document WHERE pubmed_id = "{pubmed_id}"')
        data = self.cursor.fetchall()
        if data:
            result = data[0][0]
        else:
            sql = "INSERT INTO document (title, doi, pubmed_id, pmc_id, sici, publisher_id, year, journal, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (title, doi, pubmed_id, pmc_id, sici, publisher_id, year, journal, volume)
            self.cursor.execute(sql, values)
            self.database.commit()
            doc_id = self.cursor.lastrowid
            
            sql = "INSERT INTO author (doc_id, first_name, last_name) VALUES (%s, %s, %s)"
            values = (doc_id, author_first_name, author_last_name)
            self.cursor.execute(sql, values)
            self.database.commit()
            
            result = doc_id
        return result


    def add_plaza_spec_id(self, tax_id: int, plaza_id: str):
        sql = "INSERT INTO plaza_species_id (tax_id, plaza_id) VALUES(%s, %s)"
        self.cursor.execute(sql, (tax_id, plaza_id))
        self.database.commit()


    def get_plaza_spec_id_dict(self):
        self.cursor.execute("SELECT plaza_id, tax_id FROM plaza_species_id")
        data = self.cursor.fetchall()
        return dict(data)
    
    
    def import_plaza_synonyms(self, plaza_synonyms: List):
        sql = "INSERT INTO plaza_gene_synonym (tax_id, plaza_id, synonym) VALUES(%s, %s, %s)"
        self.cursor.executemany(sql, plaza_synonyms)
        self.database.commit()


    def import_plaza_orthology(self, orthology_values: List):
        self.wake_up_connection()
        sql = "INSERT INTO plaza_orthology (query_tax_id, query_gene, ortho_tax_id, ortho_gene, ortho_type) VALUES (%s, %s, %s, %s, %s)"
        self.cursor.executemany(sql, orthology_values)
        self.database.commit()


    def get_ortho_links_between_species(self, query_spec_id: int, ortho_spec_id: int):
        self.cursor.execute(f'SELECT DISTINCT query_gene, ortho_gene, ortho_type, gs.id AS query_gene_id, gs.plaza_synonyms AS query_gene_synonyms '
                             'FROM plaza_orthology '
                             'INNER JOIN gene_synonym gs ON gs.plaza_id = query_gene '
                            f'WHERE query_tax_id = {query_spec_id} AND ortho_tax_id = {ortho_spec_id}')
        data = self.cursor.fetchall()
        return list(map(list, zip(*data)))
        
        
    def import_unlinked_plaza_genes(self, unlinked_plaza_genes: Dict):  # dict[plaza_id] = {'tax_id' : xx, 'db_xref' : yy, 'synonyms' : zz1 | zz2 }
        values = []
        for key in unlinked_plaza_genes.keys():
            values.append((unlinked_plaza_genes[key]['tax_id'], key, unlinked_plaza_genes[key]['synonyms'], unlinked_plaza_genes[key]['db_xref']))
        sql = 'INSERT INTO gene_synonym (tax_id, plaza_id, plaza_synonyms, db_xref, source) VALUES(%s, %s, %s, %s, "PLAZA")'
        self.cursor.executemany(sql, values)
        self.database.commit()
        
        
    def update_linked_plaza_genes(self, linked_plaza_genes: Dict):  # dict[plaza_id] = {'tax_id' : xx, 'db_xref' : yy, 'synonyms' : zz1 | zz2 }
        values = []
        for key in linked_plaza_genes.keys():
            values.append((linked_plaza_genes[key]['synonyms'], linked_plaza_genes[key]['db_xref'], key))
        sql = 'UPDATE gene_synonym SET plaza_synonyms = %s, db_xref = %s, source = "NCBI/PLAZA" WHERE plaza_id = %s'
        self.cursor.executemany(sql, values)
        self.database.commit()
    
    
    def get_plaza_gene_id_from_plaza_synonym(self, plaza_synonym: str, tax_id: int):
        self.cursor.execute(f'SELECT plaza_id FROM plaza_gene_synonym WHERE plaza_id = "{plaza_synonym}" OR (tax_id = {tax_id} AND synonym = "{plaza_synonym}")')
        data = self.cursor.fetchall()
        return data[0][0] if data else -1
    
    
    def get_gene_id_from_plaza_id(self, plaza_id: str):
        self.cursor.execute(f'SELECT id FROM gene_synonym WHERE plaza_id = "{plaza_id}"')
        data = self.cursor.fetchall()
        return data[0][0] if data else -1


    def extract_species_annotation_paragraph_types(self):
        """
        Extracts, for each paragraph type, the number of occurrences of species annotations.

        Returns
        -------
        list[list]
            for each paragraph type, the number of section annotation occurrences
        """
        self.cursor.execute("SELECT p.section_type, COUNT(*) AS section_type_count FROM tm_species_annotation sa INNER JOIN paragraph p ON p.id = sa.par_id GROUP BY p.section_type")
        return self.cursor.fetchall()


    def extract_gene_annotation_paragraph_types(self):
        """
        Extracts, for each paragraph type, the number of occurrences of gene annotations.

        Returns
        -------
        list[list]
            for each paragraph type, the number of gene annotation occurrences
        """
        self.cursor.execute("SELECT p.section_type, COUNT(*) AS section_type_count FROM tm_gene_annotation ga INNER JOIN paragraph p ON p.id = ga.par_id GROUP BY p.section_type")
        return self.cursor.fetchall()


    def extract_trait_annotation_paragraph_types(self):
        """
        Extracts, for each paragraph type, the number of occurrences of trait annotations.

        Returns
        -------
        list[list]
            for each paragraph type, the number of trait annotation occurrences
        """
        self.cursor.execute("SELECT p.section_type, COUNT(*) AS section_type_count FROM tm_trait_annotation ta INNER JOIN paragraph p ON p.id = ta.par_id GROUP BY p.section_type")
        return self.cursor.fetchall()


    def extract_species_distribution_per_section_type(self, section_type:str = ""):
        if section_type == "":
            self.cursor.execute("SELECT p.doc_id, COUNT(DISTINCT(sa.spec_id)) FROM tm_species_annotation sa INNER JOIN paragraph p ON p.id = sa.par_id GROUP BY p.doc_id")
        else:
            self.cursor.execute(f'SELECT p.doc_id, COUNT(DISTINCT(sa.spec_id)) FROM tm_species_annotation sa INNER JOIN paragraph p ON p.id = sa.par_id WHERE p.section_type = "{section_type}" GROUP BY p.doc_id')
        return self.cursor.fetchall()


    def extract_association_paragraph_types(self, high_quality_only: bool = False):
        """
        Extracts, for each paragraph type, the number of occurrences of associations.

        Parameters
        ----------
        high_quality_only : bool, optional
            only extract hiqh quality associations (score >= 10), by default False

        Returns
        -------
        list[list]
            for each paragraph type, the number of association occurrences
        """
        if high_quality_only:
            self.cursor.execute("SELECT p.section_type, COUNT(*) AS section_type_count FROM tm_evidence ae INNER JOIN paragraph p ON p.id = ae.par_id WHERE score >= 10 GROUP BY p.section_type")
        else:
            self.cursor.execute("SELECT p.section_type, COUNT(*) AS section_type_count FROM tm_evidence ae INNER JOIN paragraph p ON p.id = ae.par_id GROUP BY p.section_type")
        return self.cursor.fetchall()


    def extract_publications_per_year(self):
        """
        Extracts publication year distribution for the documents in the database.

        Returns
        -------
        list[list]
            publication year distribution
        """
        self.cursor.execute("SELECT DISTINCT year, COUNT(year) FROM document GROUP BY year ORDER BY year")
        data = self.cursor.fetchall()
        return list(map(list, zip(*data)))


    def get_number_of_tm_triples(self):
        self.cursor.execute('SELECT COUNT(DISTINCT (ass.id)) FROM association ass INNER JOIN tm_evidence tme ON tme.assoc_id = ass.id')
        data = self.cursor.fetchall()
        return data[0][0]

    
    def get_number_of_tm_species(self):
        self.cursor.execute('SELECT COUNT(DISTINCT (ass.spec_id)) FROM association ass INNER JOIN tm_evidence tme ON tme.assoc_id = ass.id')
        data = self.cursor.fetchall()
        return data[0][0]


    def get_number_of_tm_genes(self):
        self.cursor.execute('SELECT COUNT(DISTINCT (ass.gene_id)) FROM association ass INNER JOIN tm_evidence tme ON tme.assoc_id = ass.id')
        data = self.cursor.fetchall()
        return data[0][0]


    def get_number_of_tm_traits(self):
        self.cursor.execute('SELECT COUNT(DISTINCT (ass.trait_id)) FROM association ass INNER JOIN tm_evidence tme ON tme.assoc_id = ass.id')
        data = self.cursor.fetchall()
        return data[0][0]
    

    def get_number_of_retained_tm_triples(self):
        self.cursor.execute('SELECT COUNT(DISTINCT (ass.id)) '
                            'FROM association ass '
                            'INNER JOIN tm_evidence tme ON tme.assoc_id = ass.id '
                            'INNER JOIN gene_synonym gs ON gs.id = ass.gene_id '
                            'WHERE gs.plaza_id IS NOT NULL')
        data = self.cursor.fetchall()
        return data[0][0]


    def get_number_of_retained_tm_triples_for_tax_id(self, tax_id):
        self.cursor.execute('SELECT COUNT(DISTINCT (ass.id)) '
                            'FROM association ass '
                            'INNER JOIN tm_evidence tme ON tme.assoc_id = ass.id '
                            'INNER JOIN gene_synonym gs ON gs.id = ass.gene_id '
                            f'WHERE gs.plaza_id IS NOT NULL AND ass.spec_id = {tax_id}')
        data = self.cursor.fetchall()
        return data[0][0]


    def get_number_of_retained_triples(self):
        self.cursor.execute('SELECT COUNT(DISTINCT (ass.id)) '
                            'FROM association ass '
                            'INNER JOIN gene_synonym gs ON gs.id = ass.gene_id '
                            'WHERE gs.plaza_id IS NOT NULL')
        data = self.cursor.fetchall()
        return data[0][0]

    
    def get_number_of_retained_tm_species(self):
        self.cursor.execute('SELECT COUNT(DISTINCT (ass.spec_id)) '
                            'FROM association ass '
                            'INNER JOIN tm_evidence tme ON tme.assoc_id = ass.id '
                            'INNER JOIN gene_synonym gs ON gs.id = ass.gene_id '
                            'WHERE gs.plaza_id IS NOT NULL')
        data = self.cursor.fetchall()
        return data[0][0]


    def get_number_of_retained_species(self):
        self.cursor.execute('SELECT COUNT(DISTINCT (ass.spec_id)) '
                            'FROM association ass '
                            'INNER JOIN gene_synonym gs ON gs.id = ass.gene_id '
                            'WHERE gs.plaza_id IS NOT NULL')
        data = self.cursor.fetchall()
        return data[0][0]


    def get_retained_species(self):
        self.cursor.execute('SELECT DISTINCT (ass.spec_id) '
                            'FROM association ass '
                            'INNER JOIN gene_synonym gs ON gs.id = ass.gene_id '
                            'WHERE gs.plaza_id IS NOT NULL')
        data = self.cursor.fetchall()
        return data


    def get_number_of_retained_tm_genes(self):
        self.cursor.execute('SELECT COUNT(DISTINCT (ass.gene_id)) '
                            'FROM association ass '
                            'INNER JOIN tm_evidence tme ON tme.assoc_id = ass.id '
                            'INNER JOIN gene_synonym gs ON gs.id = ass.gene_id '
                            'WHERE gs.plaza_id IS NOT NULL')
        data = self.cursor.fetchall()
        return data[0][0]


    def get_number_of_retained_tm_genes_for_tax_id(self, tax_id: int):
        self.cursor.execute('SELECT COUNT(DISTINCT (ass.gene_id)) '
                            'FROM association ass '
                            'INNER JOIN tm_evidence tme ON tme.assoc_id = ass.id '
                            'INNER JOIN gene_synonym gs ON gs.id = ass.gene_id '
                            f'WHERE gs.plaza_id IS NOT NULL AND ass.spec_id = {tax_id}')
        data = self.cursor.fetchall()
        return data[0][0]


    def get_number_of_retained_genes(self):
        self.cursor.execute('SELECT COUNT(DISTINCT (ass.gene_id)) '
                            'FROM association ass '
                            'INNER JOIN gene_synonym gs ON gs.id = ass.gene_id '
                            'WHERE gs.plaza_id IS NOT NULL')
        data = self.cursor.fetchall()
        return data[0][0]


    def get_number_of_retained_tm_traits(self):
        self.cursor.execute('SELECT COUNT(DISTINCT (ass.trait_id)) '
                            'FROM association ass '
                            'INNER JOIN tm_evidence tme ON tme.assoc_id = ass.id '
                            'INNER JOIN gene_synonym gs ON gs.id = ass.gene_id '
                            'WHERE gs.plaza_id IS NOT NULL')
        data = self.cursor.fetchall()
        return data[0][0]


    def get_number_of_retained_tm_traits_for_tax_id(self, tax_id: int):
        self.cursor.execute('SELECT COUNT(DISTINCT (ass.trait_id)) '
                            'FROM association ass '
                            'INNER JOIN tm_evidence tme ON tme.assoc_id = ass.id '
                            'INNER JOIN gene_synonym gs ON gs.id = ass.gene_id '
                            f'WHERE gs.plaza_id IS NOT NULL AND ass.spec_id = {tax_id}')
        data = self.cursor.fetchall()
        return data[0][0]


    def get_number_of_retained_traits(self):
        self.cursor.execute('SELECT COUNT(DISTINCT (ass.trait_id)) '
                            'FROM association ass '
                            'INNER JOIN gene_synonym gs ON gs.id = ass.gene_id '
                            'WHERE gs.plaza_id IS NOT NULL')
        data = self.cursor.fetchall()
        return data[0][0]


    def get_number_of_to_term_triples(self):
        self.cursor.execute('SELECT COUNT(DISTINCT(ass.id)) FROM association ass WHERE ass.trait_id LIKE "TO%"')
        data = self.cursor.fetchall()
        return data[0][0]
    

    def get_number_of_go_term_triples(self):
        self.cursor.execute('SELECT COUNT(DISTINCT(ass.id)) FROM association ass WHERE ass.trait_id LIKE "GO%"')
        data = self.cursor.fetchall()
        return data[0][0]
    
    
    def get_number_of_ppto_term_triples(self):
        self.cursor.execute('SELECT COUNT(DISTINCT(ass.id)) FROM association ass WHERE ass.trait_id LIKE "PPTO%"')
        data = self.cursor.fetchall()
        return data[0][0]
    

    def get_number_of_to_terms(self):
        self.cursor.execute('SELECT COUNT(DISTINCT(ass.trait_id)) FROM association ass WHERE ass.trait_id LIKE "TO%"')
        data = self.cursor.fetchall()
        return data[0][0]
    

    def get_number_of_go_terms(self):
        self.cursor.execute('SELECT COUNT(DISTINCT(ass.trait_id)) FROM association ass WHERE ass.trait_id LIKE "GO%"')
        data = self.cursor.fetchall()
        return data[0][0]
    
    
    def get_number_of_ppto_terms(self):
        self.cursor.execute('SELECT COUNT(DISTINCT(ass.trait_id)) FROM association ass WHERE ass.trait_id LIKE "PPTO%"')
        data = self.cursor.fetchall()
        return data[0][0]
    

    def get_number_of_to_term_tm_triples(self):
        self.cursor.execute('SELECT COUNT(DISTINCT(ass.id)) FROM association ass INNER JOIN tm_evidence te ON te.assoc_id = ass.id WHERE ass.trait_id LIKE "TO%"')
        data = self.cursor.fetchall()
        return data[0][0]
    

    def get_number_of_go_term_tm_triples(self):
        self.cursor.execute('SELECT COUNT(DISTINCT(ass.id)) FROM association ass INNER JOIN tm_evidence te ON te.assoc_id = ass.id WHERE ass.trait_id LIKE "GO%"')
        data = self.cursor.fetchall()
        return data[0][0]
    
    
    def get_number_of_ppto_term_tm_triples(self):
        self.cursor.execute('SELECT COUNT(DISTINCT(ass.id)) FROM association ass INNER JOIN tm_evidence te ON te.assoc_id = ass.id WHERE ass.trait_id LIKE "PPTO%"')
        data = self.cursor.fetchall()
        return data[0][0]
    

    def get_number_of_to_tm_terms(self):
        self.cursor.execute('SELECT COUNT(DISTINCT(ass.trait_id)) FROM association ass INNER JOIN tm_evidence te ON te.assoc_id = ass.id WHERE ass.trait_id LIKE "TO%"')
        data = self.cursor.fetchall()
        return data[0][0]
    

    def get_number_of_go_tm_terms(self):
        self.cursor.execute('SELECT COUNT(DISTINCT(ass.trait_id)) FROM association ass INNER JOIN tm_evidence te ON te.assoc_id = ass.id WHERE ass.trait_id LIKE "GO%"')
        data = self.cursor.fetchall()
        return data[0][0]
    
    
    def get_number_of_ppto_tm_terms(self):
        self.cursor.execute('SELECT COUNT(DISTINCT(ass.trait_id)) FROM association ass INNER JOIN tm_evidence te ON te.assoc_id = ass.id WHERE ass.trait_id LIKE "PPTO%"')
        data = self.cursor.fetchall()
        return data[0][0]


    def get_number_of_tm_evidences_for_trait(self, trait_id):
        self.cursor.execute(f'SELECT COUNT(DISTINCT a.id) FROM association a INNER JOIN tm_evidence te ON te.assoc_id = a.id WHERE a.trait_id = "{trait_id}"')
        data = self.cursor.fetchall()
        return data[0][0]


    def get_number_of_tm_evidences_for_trait_and_species(self, trait_id, species_id):
        self.cursor.execute(f'SELECT COUNT(DISTINCT a.id) FROM association a INNER JOIN tm_evidence te ON te.assoc_id = a.id WHERE a.trait_id = "{trait_id}" AND a.spec_id = {species_id}')
        data = self.cursor.fetchall()
        return data[0][0]
    

    def get_tm_genes_for_traits_and_tax_id(self, traits: List, tax_id: int):
        traits = str(tuple(traits)).replace(',)',')')  # to match MySQL syntax
        self.cursor.execute(f'SELECT DISTINCT ass.gene_id FROM association ass INNER JOIN tm_evidence te ON te.assoc_id = ass.id WHERE ass.trait_id IN {traits} AND ass.spec_id = {tax_id}')
        data = self.cursor.fetchall()
        return list(map(list, zip(*data)))[0] if len(data) > 0 else []


    def extract_associations_per_case(self):
        """
        Extracts publications per year distribution.

        Returns
        -------
        list[list]
            publication per year distribution
        """
        self.cursor.execute("SELECT DISTINCT ast.description, COUNT(ae.type_id) FROM tm_evidence ae INNER JOIN tm_association_type ast ON ast.id = ae.type_id GROUP BY ast.id ORDER BY ast.description")
        data = self.cursor.fetchall()
        return list(map(list, zip(*data)))


    def extract_associations_per_paper(self):
        """
        Extracts associations per paper distribution.

        Returns
        -------
        list[list]
            associations per paper
        """
        self.cursor.execute("SELECT DISTINCT ae.doc_id, COUNT(ae.doc_id) FROM tm_evidence ae GROUP BY ae.doc_id")
        data = self.cursor.fetchall()
        return list(map(list, zip(*data)))


    def extract_associations_per_paper_and_type(self, assoc_type:str):
        """
        Extracts associations per paper and association type distribution.

        Parameters
        ----------
        assoc_type : str
            [description]

        Returns
        -------
        [type]
            [description]
        """
        self.cursor.execute(f'SELECT DISTINCT ae.doc_id, COUNT(ae.doc_id) FROM tm_evidence ae INNER JOIN tm_association_type ast ON ast.id = ae.type_id WHERE ast.description = "{assoc_type}" GROUP BY ae.doc_id')
        data = self.cursor.fetchall()
        return list(map(list, zip(*data)))


    def extract_tm_evidence_scores(self, limit: int = -1, without_d_cases: bool = False):
        """
        Extracts the list of association ids with all evidence scores associated to it.

        Parameters
        ----------
        limit : int, optional
            limit number of returned entries, by default -1
        without_d_cases : bool, optional
            remove 1d and 2d cases, by default False

        Returns
        -------
        list
            couples of (assocation - evidence score)
        """
        limit_string = f" LIMIT {limit}" if limit > 0 else ""
        if without_d_cases:
            self.cursor.execute(f'SELECT assoc_id, type_id, score FROM tm_evidence WHERE type_id NOT IN (4, 9){limit_string}')
        else:
            self.cursor.execute(f"SELECT assoc_id, type_id, score FROM tm_evidence{limit_string}")
        return self.cursor.fetchall()


    def extract_tm_evidence_scores_per_section_type(self, section_type: str = "", without_d_cases: bool = False):
        """
        Extracts the list of association ids with all evidence scores associated to it,
        for all evidences found in the given section type.

        Returns
        -------
        list
            couples of (assocation - evidence score)
        """
        if without_d_cases:
            self.cursor.execute(f'SELECT ae.assoc_id, ae.type_id, ae.score FROM tm_evidence ae INNER JOIN paragraph p ON p.id = ae.par_id WHERE p.section_type = "{section_type}" AND ae.type_id NOT IN (4,9) LIMIT 1000000')
        else:
            self.cursor.execute(f'SELECT ae.assoc_id, ae.type_id, ae.score FROM tm_evidence ae INNER JOIN paragraph p ON p.id = ae.par_id WHERE p.section_type = "{section_type}" LIMIT 1000000')
        return self.cursor.fetchall()


    def extract_top_species_in_assocs(self, limit: int):
        self.cursor.execute(f"SELECT ss.id, ss.ncbi_synonyms, COUNT(ss.id) AS cnt FROM association assoc INNER JOIN species_synonym ss ON ss.id = assoc.spec_id GROUP BY ss.id ORDER BY cnt DESC LIMIT {limit}")
        data = self.cursor.fetchall()
        return list(map(list, zip(*data)))


    def extract_top_traits_in_assocs(self, limit: int):
        self.cursor.execute(f'SELECT ts.id, ts.synonyms, COUNT(ts.id) AS cnt FROM association assoc INNER JOIN trait_synonym ts ON ts.id = assoc.trait_id WHERE ts.id LIKE "TO:%" GROUP BY ts.id ORDER BY cnt DESC LIMIT {limit}')
        data = self.cursor.fetchall()
        return list(map(list, zip(*data)))

    
    def get_gene_ids_associated_to_tax_id(self, tax_id):
        self.cursor.execute(f"SELECT DISTINCT gene_id FROM association WHERE spec_id = {tax_id}")
        data = self.cursor.fetchall()
        return {item for t in data for item in t}
    
    
    def get_genes_associated_to_tax_id(self, tax_id):
        result = []
        self.cursor.execute(f"SELECT DISTINCT gene_id, gs.tax_id, ss.ncbi_synonyms FROM association a INNER JOIN gene_synonym gs ON gs.id = a.gene_id INNER JOIN species_synonym ss ON ss.id = gs.tax_id WHERE a.spec_id = {tax_id}")
        data = self.cursor.fetchall()
        if data:
            result = list(map(list, zip(*data)))
        return result
    
    
    def get_gene_ids_associated_to_tax_id_with_consistent_tax_id(self, tax_id):
        self.cursor.execute(f"SELECT DISTINCT gene_id FROM association INNER JOIN gene_synonym gs on gs.id = gene_id WHERE spec_id = {tax_id}")
        data = self.cursor.fetchall()
        return {item for t in data for item in t}


    def get_number_of_assocs_for_species_and_trait(self, spec_id: int, trait_id: str):
        self.cursor.execute(f'SELECT COUNT(*) FROM `association` WHERE spec_id = {spec_id} AND trait_id = "{trait_id}"')
        return self.cursor.fetchone()


    def get_association_types(self):
        result = []
        self.cursor.execute("SELECT id, description FROM tm_association_type")
        data = self.cursor.fetchall()
        if data:
            result = list(map(list, zip(*data)))
        return result


    def get_assoc_ids_number(self):
        self.cursor.execute("SELECT MAX(id) FROM association")
        return self.cursor.fetchone()[0]


    def get_tm_evidences_of_type(self, type_id: int):
        result = []
        self.cursor.execute(f"SELECT DISTINCT(assoc_id) FROM tm_evidence WHERE type_id = {type_id} ORDER BY assoc_id")
        data = self.cursor.fetchall()
        if data:
            result = list(map(list, zip(*data)))[0]
        return result


    def get_assoc_info(self):
        result = []
        self.cursor.execute("SELECT a.id, a.spec_id, ss.ncbi_synonyms AS spec_synonyms, a.gene_id, gs.ncbi_synonyms AS gene_synonyms, a.trait_id, ts.synonyms AS trait_synonym "
                            "FROM association a INNER JOIN species_synonym ss ON ss.id = a.spec_id "
                            "INNER JOIN gene_synonym gs ON gs.id = a.gene_id "
                            "INNER JOIN trait_synonym ts ON ts.id = a.trait_id ")
        data = self.cursor.fetchall()
        if data:
            result = list(map(list, zip(*data)))
        return result


    def get_max_score_per_assoc(self):
        self.cursor.execute("SELECT ae.assoc_id, MAX(ae.score) FROM tm_evidence ae GROUP BY ae.assoc_id ORDER BY ae.assoc_id")
        data = self.cursor.fetchall()
        if data:
            result = list(map(list, zip(*data)))
        return result

    def get_mean_score_per_assoc(self):
        self.cursor.execute("SELECT ae.assoc_id, AVG(ae.score) FROM tm_evidence ae GROUP BY ae.assoc_id ORDER BY ae.assoc_id")
        data = self.cursor.fetchall()
        if data:
            result = list(map(list, zip(*data)))
        return result


    def get_evidences_count_per_assoc(self):
        self.cursor.execute("SELECT ae.assoc_id, COUNT(ae.assoc_id) FROM tm_evidence ae GROUP BY ae.assoc_id ORDER BY ae.assoc_id")
        data = self.cursor.fetchall()
        if data:
            result = list(map(list, zip(*data)))
        return result


    def get_1b_cases(self):
        self.cursor.execute("SELECT ae.par_id AS par_id, gs.id AS gene_id, gs.tax_id AS tax_id, sa.spec_id AS spec_id, ga.text AS gene_name, sa.text AS species_name, p.text AS text "
                            "FROM tm_evidence ae "
                            "INNER JOIN tm_gene_annotation ga ON ga.id = ae.gene_ann_id "
                            "INNER JOIN gene_synonym gs ON gs.ncbi_id = ga.gene_id "
                            "INNER JOIN paragraph p ON p.id = ae.par_id "
                            "LEFT JOIN tm_species_annotation sa ON sa.id = ae.spec_ann_id "
                            "WHERE type_id = 2")
        data = self.cursor.fetchall()
        return list(map(list, zip(*data)))


    def get_author_publications(self, first_name: str, last_name: str):
        self.cursor.execute(f'SELECT d.id, d.pubmed_id, d.title FROM author a INNER JOIN document d ON d.id = a.doc_id WHERE a.first_name="{first_name}" AND a.last_name = "{last_name}"')
        data = self.cursor.fetchall()
        return data


    def get_associations_for_document(self, doc_id: int, score: int):
        assert 0 <= score <= 100, f"invalid score provided for paragraph evidence: {score}"
        self.cursor.execute("SELECT ae.assoc_id, ae.type_id, ga.par_id AS par_id, sa.par_id AS spec_par_id, ae.trait_synonym AS trait, ga.text AS gene, sa.text AS species, ts.synonyms AS trait_synonyms, gs.ncbi_synonyms AS gene_synonyms, ss.ncbi_synonyms AS species_synonyms, ae.score, p.text "
                            "FROM tm_evidence ae "
                            "INNER JOIN tm_trait_annotation ta ON ta.id = ae.trait_ann_id "
                            "INNER JOIN trait_synonym ts ON ts.id = ta.trait_id "
                            "INNER JOIN tm_gene_annotation ga ON ga.id = ae.gene_ann_id "
                            "INNER JOIN gene_synonym gs ON gs.ncbi_id = ga.gene_id "
                            "INNER JOIN tm_species_annotation sa ON sa.id = ae.spec_ann_id "
                            "INNER JOIN species_synonym ss ON ss.id = sa.spec_id "
                            "INNER JOIN paragraph p ON p.id = ga.par_id "
                            f"WHERE doc_id={doc_id} AND ae.score>={score} "
                            "ORDER BY assoc_id")
        data = self.cursor.fetchall()
        return list(map(list, zip(*data)))
    

    def get_triples_with_tm_evidences_for_tax_id(self, tax_id: int):
        self.cursor.execute("SELECT ass.spec_id AS species_id, ass.trait_id AS term_id, ass.gene_id AS gene_id, gs.plaza_id AS gene_name, gs.plaza_synonyms AS gene_synonyms, "
                                    "d.pubmed_id AS pubmed_id, MAX(tme.score) AS max_score, COUNT(*) as ev_count "
                            "FROM association ass "
                            "INNER JOIN gene_synonym gs ON gs.id = ass.gene_id "
                            "INNER JOIN tm_evidence tme ON tme.assoc_id = ass.id "
                            "INNER JOIN document d ON d.id = tme.doc_id "
                            f"WHERE gs.plaza_id IS NOT NULL AND ass.spec_id = {tax_id} "
                            "GROUP BY species_id, term_id, gene_id, gene_name, gene_synonyms, pubmed_id")
        data = self.cursor.fetchall()
        return list(map(list, zip(*data)))


    def add_gene2accession_info(self, values: List):
        sql = "INSERT INTO ncbi_gene2accession VALUES(%s, %s, %s, %s, %s, %s)"
        self.cursor.executemany(sql, values)
        self.database.commit()


    def get_gene2accession_info_for_tax_id(self, tax_id: int):
        result = []
        try:
            self.cursor.execute(f"SELECT * FROM ncbi_gene2accession WHERE tax_id={tax_id}")
            data = self.cursor.fetchall()
            if data:
                result = list(map(list, zip(*data)))
        except Exception as exception:
            tools.print_exception_message(f"Error occurred during gene2accession info retrieval: {exception}")
        return result


    def get_gene_info_for_tax_id(self, tax_id: int):
        self.cursor.execute(f"SELECT id, ncbi_id, ncbi_synonyms, symbol, locus_tag, db_xref FROM gene_synonym WHERE tax_id={tax_id}")
        data = self.cursor.fetchall()
        return list(map(list, zip(*data)))
    
    
    def remove_ncbi_plaza_links(self):
        # first remove PLAZA genes without links to NCBI genes
        self.cursor.execute("DELETE FROM gene_synonym WHERE ncbi_id IS NULL")
        # then remove previously computed links
        self.cursor.execute("UPDATE gene_synonym SET plaza_id=NULL")
        self.database.commit()
    
    
    def get_plaza_gene_ids_for_tax_id(self, tax_id: int):
        self.cursor.execute(f"SELECT plaza_id, synonym FROM plaza_gene_synonym WHERE tax_id={tax_id}")
        data = self.cursor.fetchall()
        data = list(map(list, zip(*data)))
        return data[0] if len(data) > 0 else []


    def add_plaza_synonym(self, gene_id, plaza_gene_id):
        sql = 'UPDATE gene_synonym SET plaza_id = %s WHERE id = %s'
        self.cursor.execute(sql, (plaza_gene_id, gene_id))
        self.database.commit()


    def add_plaza_synonyms(self, values):
        for value in progress.bar(values):
            sql = 'UPDATE gene_synonym SET plaza_id = %s WHERE ncbi_id = %s'
            self.cursor.execute(sql, (value[1], value[0]))
            self.database.commit()


    def get_all_tm_evidences(self):
        self.cursor.execute("SELECT a.trait_id AS trait_id, gs.plaza_id AS gene_id, MAX(tme.score) AS max_score, COUNT(*) AS ev_count "
                            "FROM association a "
                            "INNER JOIN tm_evidence tme ON tme.assoc_id = a.id "
                            "INNER JOIN gene_synonym gs ON gs.id = a.gene_id "
                            "WHERE gs.plaza_id IS NOT NULL "
                            "GROUP BY trait_id, gene_id")
        data = self.cursor.fetchall()
        return list(map(list, zip(*data)))
    
    
    def get_tm_evidences_for_tax_id(self, tax_id: int):
        self.cursor.execute("SELECT a.trait_id AS trait_id, gs.plaza_id AS gene_id, MAX(tme.score) AS max_score, COUNT(*) AS ev_count "
                            "FROM association a "
                            "INNER JOIN tm_evidence tme ON tme.assoc_id = a.id "
                            "INNER JOIN gene_synonym gs ON gs.id = a.gene_id "
                            f"WHERE a.spec_id={tax_id} AND score <> 0 AND gs.plaza_id IS NOT NULL "
                            "GROUP BY trait_id, gene_id "
                            "ORDER BY trait_id, gene_id")
        data = self.cursor.fetchall()
        return list(map(list, zip(*data)))
    

    def get_tm_evidences_with_cases_for_tax_id(self, tax_id: int):
        self.cursor.execute("SELECT DISTINCT a.trait_id AS trait_id, gs.plaza_id AS gene_id, tat.description AS tm_type "
                            "FROM association a "
                            "INNER JOIN tm_evidence tme ON tme.assoc_id = a.id "
                            "INNER JOIN gene_synonym gs ON gs.id = a.gene_id "
                            "INNER JOIN tm_association_type tat ON tat.id = a.type_id "
                            f"WHERE a.spec_id={tax_id} AND score <> 0 AND gs.plaza_id IS NOT NULL "
                            "ORDER BY trait_id, gene_id")
        data = self.cursor.fetchall()
        return list(map(list, zip(*data)))


    def get_tm_associations_for_tax_id_with_trait_synonyms(self, tax_id: int):
        self.cursor.execute("SELECT DISTINCT gs.plaza_id AS gene_id, ts.synonyms AS trait_id "
                            "FROM association a "
                            "INNER JOIN tm_evidence tme ON tme.assoc_id = a.id "
                            "INNER JOIN gene_synonym gs ON gs.id = gene_id "
                            "INNER JOIN trait_synonym ts ON ts.id = trait_id "
                            f"WHERE a.spec_id = {tax_id} AND gs.plaza_id IS NOT NULL")
        data = self.cursor.fetchall()
        return list(map(list, zip(*data)))


    def get_tm_associations_for_tax_id_with_trait_synonyms_and_metrics(self, tax_id: int):
        self.cursor.execute("SELECT gs.plaza_id AS gene_id, ts.synonyms AS trait_id, MAX(tme.score) AS max_score, COUNT(*) AS ev_count "
                            "FROM association a "
                            "INNER JOIN tm_evidence tme ON tme.assoc_id = a.id "
                            "INNER JOIN gene_synonym gs ON gs.id = gene_id "
                            "INNER JOIN trait_synonym ts ON ts.id = trait_id "
                            f"WHERE a.spec_id = {tax_id} AND score <> 0 AND gs.plaza_id IS NOT NULL "
                            "GROUP BY trait_id, gene_id "
                            "ORDER BY trait_id, gene_id")
        data = self.cursor.fetchall()
        return list(map(list, zip(*data)))
    

    def get_tm_evidences_for_tm_case(self, tm_case, trait_identifiers, limit):
        trait_identifiers = tuple(trait_identifiers)  # to match the MySQL syntax
        if tm_case in ["1a", "2a"]:  # in the a cases the species is retrieved from the gene, so no species annotation is available
            self.cursor.execute('SELECT ss.ncbi_synonyms AS species, tga.text AS gene, gs.ncbi_synonyms AS gene_synonyms, te.trait_synonym AS trait, d.title AS title, '
                                'p.text AS paragraph, p.section_type AS section, d.doi AS doi, tat.description AS type, te.score AS score, "-1" AS species_offset, tga.offset AS gene_offset, tta.offset AS trait_offset '
                                'FROM tm_evidence te '
                                'INNER JOIN tm_association_type tat ON tat.id = te.type_id '
                                'INNER JOIN association ass ON ass.id = te.assoc_id '
                                'INNER JOIN tm_gene_annotation tga ON tga.id = te.gene_ann_id '
                                'INNER JOIN tm_trait_annotation tta ON tta.id = te.trait_ann_id '
                                'INNER JOIN species_synonym ss ON ss.id = ass.spec_id '
                                'INNER JOIN paragraph p ON p.id = te.par_id '
                                'INNER JOIN document d ON d.id = te.doc_id '
                                'INNER JOIN gene_synonym gs ON gs.id = ass.gene_id '
                               f'WHERE tat.description = "{tm_case}" AND ass.trait_id IN {trait_identifiers} '
                               f"LIMIT {limit}")
        else:
            self.cursor.execute('SELECT tsa.text AS species, tga.text AS gene, gs.ncbi_synonyms AS gene_synonyms, te.trait_synonym AS trait, d.title AS title, p.text AS paragraph, '
                                'p.section_type AS section, d.doi AS doi, tat.description AS type, te.score AS score, tsa.offset AS species_offset, tga.offset AS gene_offset, tta.offset AS trait_offset '
                                'FROM tm_evidence te '
                                'INNER JOIN tm_association_type tat ON tat.id = te.type_id '
                                'INNER JOIN association ass ON ass.id = te.assoc_id '
                                'INNER JOIN tm_species_annotation tsa ON tsa.id = te.spec_ann_id '
                                'INNER JOIN tm_gene_annotation tga ON tga.id = te.gene_ann_id '
                                'INNER JOIN tm_trait_annotation tta ON tta.id = te.trait_ann_id '
                                'INNER JOIN paragraph p ON p.id = te.par_id '
                                'INNER JOIN document d ON d.id = te.doc_id '
                                'INNER JOIN gene_synonym gs ON gs.id = ass.gene_id '
                               f'WHERE tat.description = "{tm_case}" AND ass.trait_id IN {trait_identifiers} '
                               f"LIMIT {limit}")
        data = self.cursor.fetchall()
        return list(map(list, zip(*data)))
    

    def get_tm_evidences_for_trait_list(self, trait_identifiers):
        trait_identifiers = tuple(trait_identifiers)  # to match the MySQL syntax
        self.cursor.execute('SELECT ts.synonyms, gs.plaza_id, gs.ncbi_synonyms, COUNT(*) as assoc_count, GROUP_CONCAT(DISTINCT d.pubmed_id SEPARATOR ", ") AS pubmed_ids '
                            'FROM association ass '
                            'INNER JOIN trait_synonym ts ON ts.id = ass.trait_id '
                            'INNER JOIN tm_evidence tme ON tme.assoc_id = ass.id '
                            'INNER JOIN document d ON tme.doc_id = d.id '
                            'INNER JOIN gene_synonym gs ON gs.id = ass.gene_id '
                            f'WHERE  gs.plaza_id IS NOT NULL AND ass.spec_id = 4577 AND ass.trait_id IN {trait_identifiers} '
                            'GROUP BY ts.synonyms, gs.ncbi_synonyms, gs.plaza_id '
                            'ORDER BY assoc_count DESC ')
        data = self.cursor.fetchall()
        return list(map(list, zip(*data)))


    def get_gene_trait_for_tax_id(self, trait_ids: List, tax_id: int):
        self.cursor.execute('SELECT DISTINCT gs.plaza_id, ts.synonyms '
                            'FROM association ass '
                            'INNER JOIN tm_evidence tme ON tme.assoc_id = ass.id '
                            'INNER JOIN gene_synonym gs ON gs.id = ass.gene_id '
                            'INNER JOIN trait_synonym ts ON ts.id = ass.trait_id '
                            f'WHERE ass.spec_id = {tax_id} AND gs.plaza_id IS NOT NULL AND ts.id IN {tuple(trait_ids)}')
        data = self.cursor.fetchall()
        return list(map(list, zip(*data)))


    def _add_authors_to_document(self, authors: List, doc_id):
        sql = "INSERT INTO author (doc_id, first_name, last_name) VALUES (%s, %s, %s)"
        values = []
        for author in authors:
            values.append((doc_id, author['given_names'], author['surname']))
        self.cursor.executemany(sql, values)


    def _add_paragraphs_to_document(self, paragraphs: List[PubTatorParagraph], doc_id: int):
        assert len(paragraphs) > 0, f"the number of paragraphs to add to the document with id {doc_id} is zero"

        # 1. add the paragraphs
        sql = "INSERT INTO paragraph (doc_id, section_type, text) VALUES (%s, %s, %s)"
        values = []
        for paragraph in paragraphs:
            values.append((doc_id, paragraph.type.name, paragraph.text))
        self.cursor.executemany(sql, values)
        self.database.commit()
        inserted_pars = self.cursor.rowcount
        last_par_id = self.cursor.lastrowid

        if last_par_id == 0:  # an error occurred
            tools.print_exception_message(f"Error occurred during paragraph insertion. Values are: {values}")
            
        assert last_par_id != 0, "there was a problem during paragraph insertion"

        # 2. for each paragraph, insert annotations
        inserted_par_ids = range(last_par_id, last_par_id + inserted_pars)
        for i in range (inserted_pars):
            if paragraphs[i].species_annotations:
                self._add_species_annotations_to_paragraph(paragraphs[i].species_annotations, last_par_id + i)
            if paragraphs[i].gene_annotations:
                self._add_gene_annotations_to_paragraph(paragraphs[i].gene_annotations, last_par_id + i)

        return inserted_par_ids


    def _add_species_annotations_to_paragraph(self, species_annotations: List[PubTatorAnnotation], par_id: int):
        # species synonyms are inserted once at the beginning of the program, no need to handle them here
        sql = "INSERT INTO tm_species_annotation (par_id, spec_id, offset, length, text) VALUES (%s, %s, %s, %s, %s)"
        values = []
        for annotation in species_annotations:
            for spec_id in annotation.ids:
                spec_id = self._patch_spec_id(spec_id)
                values.append((par_id, spec_id, annotation.offset, annotation.length, annotation.text))
        self.cursor.executemany(sql, values)
        self.database.commit()

    def _add_gene_annotations_to_paragraph(self, gene_annotations: List[PubTatorAnnotation], par_id: int):
        try:
            # 1. insert gene annotations
            # gene synonyms are inserted once at the beginning of the program, no need to handle them here
            sql = "INSERT INTO tm_gene_annotation (par_id, gene_id, offset, length, text) VALUES (%s, %s, %s, %s, %s)"
            values = []
            for annotation in gene_annotations:
                for gene_id in annotation.ids:
                    values.append((par_id, gene_id, annotation.offset, annotation.length, annotation.text))
            self.cursor.executemany(sql, values)
            self.database.commit()
        except mysql.connector.IntegrityError:  # gene identifier used by PubTator is not in the NCBI database, so we add it
            sql = "INSERT IGNORE INTO gene_synonym (ncbi_id, ncbi_synonyms, source) VALUES (%s, %s, %s)"  # INSERT IGNORE will only insert values if the id does not exist
            values = []
            for annotation in gene_annotations:
                for gene_id in annotation.ids:
                    values.append((gene_id, annotation.text, "PubTator"))
            self.cursor.executemany(sql, values)
            self.database.commit()
            self._add_gene_annotations_to_paragraph(gene_annotations, par_id)  # and we try again
