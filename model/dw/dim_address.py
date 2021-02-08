from model.base import Base
from sqlalchemy import Column, Integer, String, DATE

class DimAddress(Base):
    __tablename__ = 'dim_address'

    address_id = Column(Integer, primary_key=True)
    city = Column(String)
    state = Column(String)
