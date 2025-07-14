"""
A collection of custom exceptions.
"""

class ConfigError(Exception):
    """
    Raised for errors related to config parsing.

    Parameters
    ----------
    Exception : Exception
        base exception class
    """


class FileFormatError(Exception):
    """
    Raized when a parsed file does not conform to a particular format.

    Parameters
    ----------
    Exception : Exception
        base exception class
    """


class NoTraitsFoundError(Exception):
    """
    Raized when no traits could be retrieved from the database.

    Parameters
    ----------
    Exception : Exception
        base exception class
    """


class PubTatorFileContentError(Exception):
    """
    Raised when the content of a PubTator file is not consistent.

    Parameters
    ----------
    Exception : Exception
        base exception class
    """