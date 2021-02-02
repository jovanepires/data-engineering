import pandas as pd

from utils import logging
from config.source_map import SourceMap

def task_rename_columns(source, dataframe):
    """ This function load json <source> file based on <SOURCE_PATH>

    Args:
        source (str): file with the same table name maped on <source_map>

    Returns:
        pd.DataFrame: all data loaded from <source> file

    """
    source_map = SourceMap(file_name="./source_map.yml")
    old_names = dataframe.columns.to_list()
    new_names = [source_map.get(source, c) for c in old_names]
    dataframe.columns = new_names
    logging.info("source:%s renamed", source)

    return dataframe
