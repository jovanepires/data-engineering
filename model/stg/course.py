from model.base import Base
from sqlalchemy import Column, Integer, String

class Course(Base):
    __tablename__ = 'courses'

    id = Column(Integer, primary_key=True)
    name = Column(String)

