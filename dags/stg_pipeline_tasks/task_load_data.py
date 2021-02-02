import pandas as pd

from utils import logging
from settings import SOURCE_PATH

logging.basicConfig(level=logging.DEBUG)
logging.getLogger(__name__).addHandler(logging.StreamHandler())

def task_load_data(source):
    """ This function load json <source> file based on <SOURCE_PATH>

    Args:
        source (str): file with the same table name maped on <source_map>

    Returns:
        pd.DataFrame: all data loaded from <source> file

    """
    path = "{path}{file}.json".format(path=SOURCE_PATH, file=source)
    data = pd.read_json(path, orient="records")
    logging.info("source:%s loaded with shape:%s", source, data.shape)

    return data
