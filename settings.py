import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_NAME = os.getenv("DATABASE_NAME", "staging.db")
SOURCE_PATH = os.getenv("SOURCE_PATH", "./data/files/")