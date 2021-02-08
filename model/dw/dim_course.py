from model.base import Base
from sqlalchemy import Column, Integer, String

class DimCourse(Base):
    __tablename__ = 'dim_course'

    course_id = Column(Integer, primary_key=True)
    name = Column(String)

