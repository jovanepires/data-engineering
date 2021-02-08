from model.base import Base
from sqlalchemy import Column, Integer, String

class DimSubject(Base):
    __tablename__ = 'dim_subject'

    subject_id = Column(Integer, primary_key=True)
    name = Column(String)
