"""
Class implementing an interface to SpaCy library.
"""

import os
import pickle
from typing import Dict
import subprocess
import sys

import spacy
from spacy.matcher import PhraseMatcher

from tools import tools


class SpacyTextAnalyzer():
    """
    Class implementing an interface to SpaCy library.
    """

    def __init__(self, terms_dict: Dict, tmp_dir: str) -> None:
        """
        Constructor.

        Initializes spaCy PhraseMatcher. As its computation from the terms dictionary
        is time consuming (taking at least half an hour), it is computed once
        and the resulting object is serialized. The next time, if the serialized
        object is detected, it is directly loaded in the memory.

        Parameters
        ----------
        terms_dict : Dict
            dictionary of terms of interest in the form {'id' : synonym1 | synonym2 | ...}
        tmp_dir : str
            path to the temporary directory
        """
        self.verbose = False

        matcher_file_name = f"{tmp_dir}/spacy-matcher.pkl"
        nlp_file_name = f"{tmp_dir}/spacy-nlp.pkl"

        if os.path.exists(matcher_file_name) and os.path.exists(nlp_file_name):
            tools.print_info_message(f"Loading spaCy PhraseMatcher from {matcher_file_name}...")
            with open(matcher_file_name, "rb") as matcher_file:
                self.matcher = pickle.load(matcher_file)
            tools.print_info_message(f"Loading spaCy nlp from {nlp_file_name}...")
            with open(nlp_file_name, "rb") as nlp_file:
                self.nlp = pickle.load(nlp_file)
        else:
            try:
                self.nlp = spacy.load("en_core_web_sm")
            except OSError:
                # install the model on-the-fly
                subprocess.check_call([sys.executable, "-m", "spacy", "download", "en_core_web_sm"])
                self.nlp = spacy.load("en_core_web_sm")

            self.matcher = PhraseMatcher(self.nlp.vocab, attr="LEMMA")
            tools.print_info_message("Preparing spaCy PhraseMatcher...")
            for (key, value) in terms_dict.items():
                synonyms = value.split('|')
                self.matcher.add(key, [self.nlp(synonym.strip().lower()) for synonym in synonyms])

            tools.print_info_message(f"Storing spaCy PhraseMatcher to {matcher_file_name}...")
            with open(matcher_file_name, "wb") as matcher_file:
                pickle.dump(self.matcher, matcher_file)
            tools.print_info_message(f"Storing spaCy nlp to {nlp_file_name}...")
            with open(nlp_file_name, "wb") as nlp_file:
                pickle.dump(self.nlp, nlp_file)
                
            tools.print_info_message("Done.")
            
            
            
    def extract_term_matches_from_text(self, text: str):
        """
        Identifies terms (specified in the dictionary provided in the constructor)
        in the provided text and returns the list of identified matches.

        Parameters
        ----------
        text : str
            text in which to search for matches

        Returns
        -------
        List[Dict]
            list of matches of the form {id - synonym - start - length}
        """
        doc = self.nlp(text.lower())
        matches = self.matcher(doc)
        
        result = []
        
        for match_id, start, end in matches:
            span = doc[start:end]
            term_id = self.nlp.vocab.strings[match_id]
            synonym = span.text
            if self.verbose:
                tools.print_info_message(f"{self.nlp.vocab.strings[match_id]} - {span.text} - {start} - {end}")
            if len(span) == 1 and span[0].pos_ not in ["NOUN", "PROPN"]:
                if self.verbose:
                    tools.print_warning_message(f"The span '{span.text.upper()}' is skipped as its POS is {span[0].pos_}.")
            else:
                result.append({'id' : term_id, 'synonym' : synonym, 'start' : span.start_char, 'length' : span.end_char - span.start_char })
                
                
        return result



    def extract_sentences(self, text: str):
        """
        Returns the list of sentences of the provided text.

        Parameters
        ----------
        text : str
            a text

        Returns
        -------
        List[str]
            list of sentences of the provided text
        """
        doc = self.nlp(text)
        return list(doc.sents)
    