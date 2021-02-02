import sys
import logging
from datetime import datetime

# set up logging to file - see previous section for more details

filename = datetime.now().strftime("worker_%d_%m_%Y.log")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-8s %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%S%z")

# define a Handler which writes INFO messages or higher to the sys.stderr
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
# set a format which is simpler for console use
formatter = logging.Formatter("%(levelname)-8s %(message)s")
# tell the handler to use this format
console.setFormatter(formatter)
# add the handler to the logger
logging.getLogger(__name__).addHandler(console)