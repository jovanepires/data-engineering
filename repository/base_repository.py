import pandas as pd

from sqlalchemy import create_engine
from settings import DATABASE_NAME
from utils import logging

logging.getLogger(__name__)

class BaseRepository():
    def __init__(self, table):
        self._eng = create_engine("sqlite:///{}".format(DATABASE_NAME))
        self._conn = None
        self._table = table

    def connect_db(self):
        """
        function to connection to database
        """
        try:
            self._conn = self._eng.connect()
            logging.info("...successfully connected to db")
        except Exception as e:
            logging.info("...unsuccessful connection: {}".format(e))
        
        return self

    def insert(self, dataframe):
        try:
            with self._conn as conn:
                dataframe.to_sql(self._table, con=self._conn, if_exists='replace')
                logging.info("...successfully inserted to db")
        except Exception as e:
            logging.info("...unsuccessful insert: {}".format(e))

    def list(self, index_col='date'):
        """
        load and clean the db data
        """
        with self._conn as conn:
            return pd.read_sql_table(self._table, conn, index_col=index_col)

