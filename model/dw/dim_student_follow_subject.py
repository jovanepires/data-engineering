from model.base import Base
from sqlalchemy import Column, Integer, String, TIMESTAMP

class DimStudentFollowSubject(Base):
    __tablename__ = 'dim_student_follow_subject'

    id = Column(Integer, primary_key=True)
    student_id = Column(String)
    subject_id = Column(Integer)
    follow_date_id = Column(Integer)
