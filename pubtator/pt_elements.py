"""
Classes describing entities extracted from PubTator documents.
A PubTator document is composed of passages, corresponding to paragraphs.
A passage is composed of:
    - infons: characteristics of the paragraph
    - text: the text of the paragraph
    - annotations: annotations of the paragraph
    - offset: absolute offset of the paragraph
An annotation is composed of:
    - type (gene, species, ...)
    - location (text + offset)
    - identifier
    - text
"""

from typing import List, Dict
import enum
import xml.etree.ElementTree as ET

from tools import tools


class AnnotationType(enum.Enum):
    """ Represents all possible PubTator annotations.
    """
    GENE = 0
    SPECIES = 1
    CHEMICAL = 2
    DISEASE = 3
    SNP = 4
    CELLLINE = 5
    PROTEINMUTATION = 6
    DNAMUTATION = 7
    DOMAINMOTIF = 8
    COPYNUMBERVARIANT = 9
    GENOMICREGION = 10
    CHROMOSOME = 11
    DNAACIDCHANGE = 12
    REFSEQ = 13
    UNKNOWN = 14
    STRAIN = 15
    MUTATION = 16
    ERROR = 17



class PubTatorAnnotation:
    """
    Represents a PubTator annotation.
    Annotations can be of different types, described in the AnnotationType enum.
    """
    
    def __init__(self, annotation_xml_node: ET.Element, paragraph, paragraph_offset: int):
        """
        Constructor.

        Parameters
        ----------
        annotation_xml_node : ET.Element
            XML node representing a PubTator annotation
        paragraph
            paragraph to which is associated the annotation
        paragraph_offset : int
            offset of the paragraph containing this annotation, relative to the beginning of the document
        """
        infons = annotation_xml_node.findall('infon')
        self.paragraph = paragraph
        self.ids = set()
        for infon in infons:
            if infon.attrib['key'] == 'type':
                try:
                    self.type = AnnotationType[infon.text.upper()]
                except Exception:
                    self.type = AnnotationType.UNKNOWN
                    tools.print_warning_message(f"Unknown annotation type: {infon.text}")
            elif infon.attrib['key'] == 'identifier':
                if infon.text is None:  # a new bug in PubTator annotations: some identifiers
                    self.type = AnnotationType.ERROR  # put the "ERROR" type so this annotation will not be used
                    break
                elif ';' in infon.text:  # there are several identifiers associated to this annotation
                    self.ids.update(infon.text.split(';'))
                else:
                    self.ids.add(infon.text)
        location = annotation_xml_node.find('location')
        self.offset = int(location.attrib['offset']) - paragraph_offset  # we want the offset to be relative to the paragraph
        self.length = int(location.attrib['length'])
        self.text = annotation_xml_node.find('text').text


    def __repr__(self) -> str:
        return f"Annotation: ids: {self.ids} -- text: {self.text}"


    def __eq__(self, another_annotation: object) -> bool:
        return self.ids == another_annotation.ids and self.offset == another_annotation.offset


    def is_gene_annotation(self):
        """
        Indicates if the current annotation is a gene annotation.

        Returns
        -------
        Bool
            indicates if the current annotation is a gene annotation
        """
        return self.type == AnnotationType.GENE and "None" not in self.ids


    def is_species_annotation(self):
        """
        Indicates if the current annotation is a species annotation.

        Returns
        -------
        Bool
            indicates if the current annotation is a species annotation
        """
        return self.type == AnnotationType.SPECIES



class ParagraphType(enum.Enum):
    """
    Represents all possible PubTator paragraph types.
    Normally, a paragraph type represents the section it belongs to (title, abstract, ...).
    But some articles don't have sections and only have custom titles.
    In that case, PubTator uses custom types (marked with '##' below).
    """
    TITLE = 0
    TITLE_1 = 1 ##
    TITLE_2 = 2 ##
    TITLE_3 = 3 ##
    TITLE_4 = 4 ##
    TITLE_5 = 5 ##
    TITLE_CAPTION = 6 ##
    ABSTRACT = 7
    ABSTRACT_TITLE_1 = 8 ##
    INTRO = 9
    RESULTS = 10
    DISCUSS = 11
    CONCL = 12
    CASE = 13
    SUPPL = 14
    APPENDIX = 15
    PARAGRAPH = 16 ##
    METHODS = 17
    FIG = 18
    FIG_CAPTION = 19 ##
    FIG_CAPTION_TITLE = 20 ##
    TABLE = 21
    TABLE_CAPTION = 22 ##
    TABLE_FOOTNOTE = 23 ##
    TABLE_CAPTION_TITLE = 24 ##
    COMP_INT = 25
    ABBR = 26
    REF = 27
    AUTH_CONT = 28
    ACK_FUND = 29
    KEYWORD = 30
    REVIEW_INFO = 31
    FRONT = 32 ##
    FOOTNOTE = 33 ##
    UNDEFINED = 34  # defined internally, to handle paragraphs with newly added types

SECTION_IGNORE_INDEX = 17  # sections with that type and types of greater value are ignored



class PubTatorParagraph:
    """
    Represents a PubTator paragraph.
    PubTator paragraphs can be of different types, described in the ParagraphType enum.
    A paragraph is composed of:
            - type
            - text
            - list of species annotations
            - list of gene annotations
    """

    def __init__(self, passage_xml_node: ET.Element, species_dict: Dict, species_synonyms_black_list: List, gene_synonyms_black_list: List, offset: int):
        """
        Constructor.
        Creates a paragraph from the provided XML node.
        The attribute 'is_relevant' indiicates whether the paragraph should be considered
        for further processing (relevant type, text non empty and of adequate length).

        Parameters
        ----------
        passage_xml_node : ET.Element
            XML node representing a PubTator paragraph
        species_dict : Dict
            dictionary of relevant species, to filter species annotations
        species_synonyms_black_list : List
            black list of species synonyms
        gene_synonyms_black_list : List
            black list of gene synonyms
        offset : int
            offset of this paragraph, relative to the beginning of the document
        """
        self.xml_node = passage_xml_node
        self.is_relevant = False
        infons = self.xml_node.findall('infon')  # infons describe paragraph properties
        section_type_infons = [infon for infon in infons if infon.attrib['key'] == 'section_type']
        if len(section_type_infons) == 0:  # this is an abstract (not a full text document)
            # EXPLANATION
            # the 'section_type' infon is only present in the full text articles
            # all the passage nodes also have a 'type' infon, specifying the type of the text
            # ex: for 'section_type' "METHODS" the 'type' could be "title" or "paragraph"
            # for articles only having abstracts, only the 'type' infon is present
            # for titles and abstracts, 'type' corresponds to 'section_type', so both infons can be used
            section_type_infons = [infon for infon in infons if infon.attrib['key'] == 'type']
        if len(section_type_infons) == 1:
            try:
                self.type = ParagraphType[section_type_infons[0].text.upper()]
            except:
                self.type = ParagraphType.UNDEFINED
                tools.print_error_message(f"Unknown paragraph type detected: {section_type_infons[0].text.upper()}")
            if self.type.value < SECTION_IGNORE_INDEX:  # paragraphs with non relevant types are discarded
                # all the sub-title types are ignored and considered as a simple paragraph
                if self.type in [ParagraphType.TITLE_1, ParagraphType.TITLE_2, ParagraphType.TITLE_3,
                                 ParagraphType.TITLE_4, ParagraphType.TITLE_CAPTION, ParagraphType.ABSTRACT_TITLE_1]:
                    self.type = ParagraphType.PARAGRAPH
                text_node = self.xml_node.find('text')
                self.text = text_node.text if hasattr(text_node, 'text') else ""
                if self.text != "" and len(self.text) < 20_000:  # long paragraphs are filtered, as generally they correspond to bad formatted text
                    if (self.type == ParagraphType.TITLE) or len(self.text.split(" ")) > 3:  # short paragraphs are filtered, except if this is a title
                        self.species_annotations = []
                        self.gene_annotations = []
                        self._retrieve_annotations(species_dict, species_synonyms_black_list, gene_synonyms_black_list, offset)
                        if len(self.gene_annotations) > 0 or self.type in [ParagraphType.TITLE, ParagraphType.ABSTRACT]:
                            self.is_relevant = True


    def _retrieve_annotations(self, species_dict: Dict, species_synonyms_black_list: List, gene_synonyms_black_list: List, offset: int):
        """
        Retrieves species and gene annotations.
        Species annotations are only retrieved if:
        - their id are in species_dict
        - their text is not in the species synonyms black list
        - their text is longer than two characters

        Parameters
        ----------
        species_dict : dict
            dictionary of relevant species
        species_synonyms_black_list : List
            black list of species synonyms
        gene_synonyms_black_list : List
            black list of gene synonyms
        offset : int
            offset of the paragraph, relative to the beginning of the document
        """
        annotations = self.xml_node.findall("annotation")
        for annotation in annotations:
            new_annotation = PubTatorAnnotation(annotation, self, offset)
            if new_annotation.is_species_annotation():
                new_annotation.ids = [id for id in new_annotation.ids if id in species_dict]  # only keep ids of relevant species
                if len(new_annotation.ids) > 0:
                    if new_annotation.text.lower() not in species_synonyms_black_list and len(new_annotation.text) > 2:
                        self.species_annotations.append(new_annotation)
            elif new_annotation.is_gene_annotation() and new_annotation.text.lower() not in gene_synonyms_black_list and new_annotation not in self.gene_annotations:
                self.gene_annotations.append(new_annotation)



class PubTatorDocument:
    """
    Represents a PubTator document.
    A document is composed of a set of properties and a list of paragraphs.
    """

    def __init__(self, document_xml_node: ET.Element, species_dict: Dict, species_synonyms_black_list: List, gene_synonyms_black_list: List):
        """
        Constructor.

        Parameters
        ----------
        document_xml_node : ET.Element
            XML node representing a PubTator document
        species_dict : Dict
            dictionary of relevant species
        species_synonyms_black_list : List
            black list of species synonyms
        gene_synonyms_black_list : List
            black list of gene synonyms
        """
        self.xml_node = document_xml_node
        self.species_dict = species_dict
        self.species_synonyms_black_list = species_synonyms_black_list
        self.gene_synonyms_black_list = gene_synonyms_black_list

        # initialization of document properties
        passage = self.xml_node.find("passage")  # document properties are stored in the first passage node
        infons = passage.findall("infon")
        self.pubmed_id = next((infon.text for infon in infons if infon.attrib['key'] == 'article-id_pmid'), "")
        if self.pubmed_id == "":  # from my observations: document id = PMC id, but if PMC id is empty, then document id = PubMed id
            self.pubmed_id = self.xml_node.find("id").text
        self.pmc_id = next((infon.text for infon in infons if infon.attrib['key'] == 'article-id_pmc'), "")
        if self.pmc_id.startswith("PMC"):  # in some cases the numeric PMC values is preceeded by "PMC"
            self.pmc_id = self.pmc_id[3:]
        self.sici = next((infon.text for infon in infons if infon.attrib['key'] == 'article-id_sici'), "")
        self.publisher_id = next((infon.text for infon in infons if infon.attrib['key'] == 'article-id_publisher-id'), "")
        self.year = next((infon.text for infon in infons if infon.attrib['key'] == 'year'), "")
        self.doi = next((infon.text for infon in infons if infon.attrib['key'] == 'article-id_doi'), "")
        journal = next((infon.text for infon in infons if infon.attrib['key'] == 'journal'), "")
        self.journal = journal.split(';')[0]  # journal is in the form "PLoS Genet.; 2007 Nov; 3(11) 194 doi:XXX", here we only want its name
        if self.doi == "" and "doi:" in journal:  # doi field is sometimes empty, in that case we try to extract it from journal
            self.doi = journal.split("doi:",1)[1]
        self.volume = next((infon.text for infon in infons if infon.attrib['key'] == 'volume'), "")
        title = passage.find('text')
        self.title = title.text if title is not None else ""  # some documents (61 on 15th Nov 2021) do not have a title

        # extraction of author related information
        # authors are in the form "surname:Albai;given-names:Giuseppe"
        author_nodes = [infon.text for infon in infons if infon.attrib['key'].startswith('name_')]
        self.authors = []
        for author_node in author_nodes:
            names = author_node.split(';')
            self.authors.append({'surname':names[0].split(':')[1], 'given_names':(names[1].split(':')[1] if len(names) >= 2 else "")})

        self.relevant_species_ids = {}  # dictionary of relevant species ids in the document identified by PubTator, with the number of their occurrences
        self.relevant_species_annotations_in_title = []  # list of relevant species annotations present in the document title, used to create "d" cases
        self.relevant_species_annotations_in_abstract = []  # list of relevant species annotations present in the document abstract, used to create "d" cases
        self.gene_ids = {}  # dictionary of genes identified by PubTator, with the number of their occurrences
        self.paragraphs = self._retrieve_paragraphs()
        self.verbose = False
        if self.verbose:
            tools.print_info_message(f"ID: {self.document_id} - TITLE: {self.document_title}, SPECIES: {len(self.relevant_species_ids)}")


    def is_relevant(self):
        """
        A document is relevant if it has relevant paragraphs
        or annotations with relevant species (provided by the species dictionary).

        Returns
        -------
        bool
            indicates whether the document is relevant
        """
        return len(self.paragraphs) > 0 and len(self.relevant_species_ids) > 0


    def _retrieve_paragraphs(self):
        """
        Extracts the list of relevant paragraphs.
        Creates the dictionary of relevant species ids of that document.
        Creates the dictionary of all gene ids of that document.

        Returns
        -------
        list
            list of relevant paragraphs
        """
        passages = self.xml_node.findall("passage")
        paragraphs = []
        for passage in passages:
            new_paragraph = PubTatorParagraph(passage, self.species_dict, self.species_synonyms_black_list, self.gene_synonyms_black_list, int(passage.find("offset").text))
            if new_paragraph.is_relevant:
                paragraphs.append(new_paragraph)
                self._retrieve_annotation_ids(new_paragraph.species_annotations, self.relevant_species_ids)
                self._retrieve_annotation_ids(new_paragraph.gene_annotations, self.gene_ids)
                if new_paragraph.type == ParagraphType.TITLE:
                    self.relevant_species_annotations_in_title.extend(new_paragraph.species_annotations)
                elif new_paragraph.type == ParagraphType.ABSTRACT:
                    self.relevant_species_annotations_in_abstract.extend(new_paragraph.species_annotations)
        return paragraphs


    def _retrieve_annotation_ids(self, annotations: List[PubTatorAnnotation], id_dict: Dict):
        """
        From the list of annotations, extracts ids of their elements and adds them
        to the privided dictionary.

        Parameters
        ----------
        annotations : List[PubTatorAnnotation]
            list of PubTator annotations
        id_dict : dict
            dictionary of ids, with the number of occurrences of each id
        """
        for annotation in annotations:
            for some_id in annotation.ids:
                id_dict.setdefault(some_id, 0)
                id_dict[some_id] += 1
