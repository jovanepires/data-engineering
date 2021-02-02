from utils import logging
from config.source_map import SourceMap

def task_list_sources():
    source_map = SourceMap(file_name="./source_map.yml")
    logging.info("source map: loaded")
    return source_map.list()