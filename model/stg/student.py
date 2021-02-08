from model.base import Base
from sqlalchemy import Column, Integer, String, TIMESTAMP

class Student(Base):
    __tablename__ = 'students'

    id = Column(String, primary_key=True)
    registered_date = Column(TIMESTAMP)
    state = Column(String)
    city = Column(String)
    university_id = Column(Integer)
    course_id = Column(Integer)
    signup_source = Column(String)
    student_client = Column(String)

