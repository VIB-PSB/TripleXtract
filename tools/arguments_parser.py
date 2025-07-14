"""
Parses command line arguments.
"""

import argparse

# pylint: disable=missing-function-docstring


class ArgumentsParser:
    """
    Parses command line arguments.
    """

    def __init__(self):
        """
        Constructor
        """
        self.argument_parser=argparse.ArgumentParser(prog = "SGT-Collector",
            description="Collects 'species-gene-trait' triples from various sources.",
            epilog="For questions and suggestions please contact svluk@psb.vib-ugent.be",
            formatter_class=SgtHelpFormatter)
        self._initialize_arguments()
        self.arguments = None


    @property
    def config_file_name(self):
        return self.arguments.config
    
    @property
    def import_species(self):
        return self.arguments.import_species
    
    @property
    def import_genes(self):
        return self.arguments.import_genes
    
    @property
    def import_to(self):
        return self.arguments.import_to
    
    @property
    def import_go(self):
        return self.arguments.import_go
    
    @property
    def import_gene2acc(self):
        return self.arguments.import_gene2acc
    
    @property
    def import_plaza_synonyms(self):
        return self.arguments.import_plaza_syn

    @property
    def compute_plaza_links(self):
        return self.arguments.plaza_links
    
    @property
    def import_pubtator_annotations(self):
        return self.arguments.import_pbt_ann

    @property
    def pubtator_file_name_pattern(self):
        return self.arguments.pbt_file_name_pattern

    @property
    def pubtator_start_doc_idx(self):
        return self.arguments.pbt_start_doc_idx

    @property
    def pubtator_end_doc_idx(self):
        return self.arguments.pbt_end_doc_idx
    
    @property
    def clear_tmp(self):
        return self.arguments.clear_tmp
        
    def parse_arguments(self):
        self.arguments = self.argument_parser.parse_args()
        

    def _initialize_arguments(self):
        self.argument_parser.add_argument("config", type=str, help="path to the config file")
        self.argument_parser.add_argument("-s", "--import_species", action="store_true", help="import NCBI species taxonomy")
        self.argument_parser.add_argument("-g", "--import_genes", action="store_true", help="import NCBI gene identifiers")
        self.argument_parser.add_argument("-t", "--import_to", action="store_true", help="import trait ontology")
        self.argument_parser.add_argument("-o", "--import_go", action="store_true", help="import gene ontology")
        self.argument_parser.add_argument("-n", "--import_ppto", action="store_true", help="import plant phenotype and trait ontology")
        self.argument_parser.add_argument("--import_gene2acc", action="store_true", help="import the NCBI gene2accession file")
        self.argument_parser.add_argument("--import_plaza_syn", action="store_true", help="import gene synonyms uzed in PLAZA")
        self.argument_parser.add_argument("-z", "--plaza_links", action="store_true", help="compute links between NCBI and PLAZA gene identifiers")
        self.argument_parser.add_argument("-p", "--import_pbt_ann", action="store_true", help="import PubTator document annotations")
        self.argument_parser.add_argument("--pbt_file_name_pattern", type=str, help="path to the files containing PubTator annotations, file index must be replaced by 'XXXXX' (e.g. BioCXML/XXXXX.BioC.XML)")
        self.argument_parser.add_argument("--pbt_start_doc_idx", type=int, help="starting PubTator document index (only if -p is specified)")
        self.argument_parser.add_argument("--pbt_end_doc_idx", type=int, help="ending PubTator document index (only if -p is specified)")
        self.argument_parser.add_argument("-c", "--clear_tmp", action="store_true", help="clear temporary files")
        


class SgtHelpFormatter(argparse.HelpFormatter):
    """
    Allows to define a custom order for the arguments.
    Inspired from https://stackoverflow.com/questions/26985650/argparse-do-not-catch-positional-arguments-with-nargs/26986546#26986546

    Args:
        argparse (argparse.HelpFormatter): base argparse HelpFormatter class
    """
    def _format_usage(self, usage, actions, groups, prefix):
        if prefix is None:
            prefix = 'usage: '

        # if usage is specified, use that
        if usage is not None:
            usage = usage % dict(prog=self._prog)

        # if no optionals or positionals are available, usage is just prog
        elif usage is None and not actions:
            usage = f"{self._prog}"
        elif usage is None:
            prog = f"{self._prog}"
            # build full usage string
            actions = actions[1:2]+actions[0:1]+actions[2:]
            action_usage = self._format_actions_usage(actions, groups) # NEW
            usage = ' '.join([s for s in [prog, action_usage] if s])
            # omit the long line wrapping code
        # prefix with 'usage:'
        return f"{prefix}{usage}\n\n"
