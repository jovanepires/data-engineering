from model.base import Base
from sqlalchemy import Column, Integer, String, TIMESTAMP

class StudentFollowSubject(Base):
    __tablename__ = 'student_follow_subject'

    id = Column(Integer, primary_key=True)
    student_id = Column(Integer)
    subject_id = Column(Integer)
    follow_date = Column(TIMESTAMP)
