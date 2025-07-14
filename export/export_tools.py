"""
Collection of tools needed for export.
"""

import pandas as pd

from tools import tools
from tools.database_handler import DatabaseHandler


def get_assocs_vs_cases_df(db_handler: DatabaseHandler):
    """
    Creates a dataframe associating TM triple evidences
    to their cases.

    Parameters
    ----------
    db_handler : DatabaseHandler
        _description_

    Returns
    -------
    _type_
        _description_
    """
    assoc_ids_number = db_handler.get_assoc_ids_number()
    dataframe = pd.DataFrame(range(1,assoc_ids_number+1))
    dataframe.columns = ['assoc_id']
    assoc_types = db_handler.get_association_types()
    dataframe[assoc_types[1]] = False
    dataframe.set_index('assoc_id', inplace=True)
    for idx, type_id in enumerate(assoc_types[0]):
        tools.print_info_message(f"--> for case {assoc_types[1][idx]}")
        assocs_vs_cases = db_handler.get_tm_evidences_of_type(type_id)
        dataframe.loc[assocs_vs_cases, assoc_types[1][idx]] = True
    return dataframe
