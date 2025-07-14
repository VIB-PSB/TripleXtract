"""
Allows to retrieve information using REST API.
"""

from typing import Dict
import urllib

import requests

from tools import tools


class RestApiHandler:
    """
    Allows to retrieve information using REST API.
    """
    
    @staticmethod
    def perform_url_request(url: str, parameters: Dict, verbose: bool = True):
        """
        Performs an REST API request using the provided parameters.

        Parameters
        ----------
        url : str
            URL string
        parameters : Dict
            parameters for the REST API request
        verbose: bool
            indicates whether status messages should be displayed

        Returns
        -------
        Dict
            content of the page in JSON format
        """
        url = f"{url}?{urllib.parse.urlencode(parameters)}"
        if verbose:
            tools.print_info_message(f"Querying '{url}'...")
        request = requests.get(url, headers={ "Content-Type" : "application/json"}, timeout=10)
        if not request.ok:
            request.raise_for_status()
        content = request.json()
        return content
