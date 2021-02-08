from model.base import Base
from sqlalchemy import Column, Integer, String

class Subject(Base):
    __tablename__ = 'subjects'

    id = Column(Integer, primary_key=True)
    name = Column(String)
