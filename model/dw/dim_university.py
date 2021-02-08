from model.base import Base
from sqlalchemy import Column, Integer, String

class DimUniversity(Base):
    __tablename__ = 'dim_university'

    university_id = Column(Integer, primary_key=True)
    name = Column(String)

