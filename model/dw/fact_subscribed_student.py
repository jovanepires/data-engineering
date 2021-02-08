from model.base import Base
from sqlalchemy import Column, Integer, String

class FactSubscribedStudent(Base):
    __tablename__ = 'fact_subscribed_student'

    id = Column(Integer, primary_key=True)
    student_id = Column(String)
    time_id = Column(Integer)
    address_id = Column(Integer)
    university_id = Column(Integer)
    course_id = Column(Integer)
    plan_type_id = Column(Integer)
