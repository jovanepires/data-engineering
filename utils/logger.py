import sys
import logging
from datetime import datetime
from settings import LOG_LEVEL

filename = datetime.now().strftime("worker_%d_%m_%Y.log")
logging.basicConfig(level=LOG_LEVEL,
                    format="%(asctime)s %(levelname)-8s %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%S%z")
