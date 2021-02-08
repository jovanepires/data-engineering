import os
import logging
from dotenv import load_dotenv

load_dotenv()


STG_DATABASE_NAME = os.getenv("STG_DATABASE_NAME", "./data/db/staging.db")
DW_DATABASE_NAME = os.getenv("DW_DATABASE_NAME", "./data/db/data_warehouse.db")
LOG_LEVEL=logging.INFO