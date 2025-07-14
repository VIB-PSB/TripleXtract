"""
HTML parser allowing to parse the provided URL and to extract URLs containing the specified string.
"""

from html.parser import HTMLParser  # https://docs.python.org/3/library/html.parser.html
from typing import List, Tuple


class CustomHtmlParser(HTMLParser):  # pylint: disable=abstract-method
    """
    HTML parser allowing to parse the provided URL and to extract URLs containing the specified string.

    Parameters
    ----------
    HTMLParser : HTMLParser
        standard HTML parser
    """
    
    def __init__(self, link_sub_string: str):
        """
        Constructor.

        Parameters
        ----------
        link_sub_string : str
            sub-string that must be present in the extracted URLs
        """
        self.file_links = []
        self.link_sub_string = link_sub_string
        HTMLParser.__init__(self)


    def handle_starttag(self, tag: str, attrs: List[Tuple[str, str]]) -> None:
        """
        Method called when an HTML start tag is encountered.
        If the start tag corresponds to a link with the desired content, stores that link.

        Parameters
        ----------
        tag : str
            the tag to parse
        attrs : List[Tuple[str, str]]
            list of attributes
        """
        if tag == "a":
            for attr in attrs:
                if attr[0] == "href" and self.link_sub_string in attr[1]:  # the link contains the specified sub-string
                    self.file_links.append(attr[1])
        return super().handle_starttag(tag, attrs)
