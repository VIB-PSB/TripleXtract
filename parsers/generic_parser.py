"""
This is a generic parser, which is not supposed to be instantiated.
To use the parser, a new class should be created, which inherits generic parser.
The children classes should implement "parse_files" method, which should parse
provided files and create a dictionary of form dict[id] = list[synonym].
The created dictionary can then be serialized and deserialized, to avoid parsing time.
"""

from tools import tools


class GenericParser:
    """
    This is a generic parser, which is not supposed to be instantiated.
    To use the parser, a new class should be created, which inherits generic parser.
    The children classes should implement "parse_files" method, which should parse
    provided files and create a dictionary of form dict[id] = list[synonym].
    """

    def __init__(self, verbose: bool = True) -> None:
        """
        Parses the provided file(s) and builds the dictionary.

        Parameters
        ----------
        verbose : bool, optional
            [description], by default True
        """
        self._verbose = verbose
        self.dictionary = {}

        try:
            self.parse_files()
        except Exception as exception:
            tools.print_exception_message(f"Error while creating the dictionary: {exception}")


    def parse_files(self):
        """
        Abstract method to parse files.

        Raises
        ------
        NotImplementedError
            raized if the inherited class didn't implement the method
        """
        raise NotImplementedError("The child class of NcbiGenericParser didn't implement ParseFiles method.")
