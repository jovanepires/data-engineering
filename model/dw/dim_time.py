from model.base import Base
from sqlalchemy import Column, Integer, String, DATE

class DimTime(Base):
    __tablename__ = 'dim_time'

    id = Column(Integer, primary_key=True)
    date = Column(DATE)
    month = Column(Integer)
    quarter = Column(Integer)
    year = Column(Integer)

