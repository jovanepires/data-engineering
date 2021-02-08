from model.base import Base
from sqlalchemy import Column, Integer, String, TIMESTAMP

class Subscription(Base):
    __tablename__ = 'subscriptions'

    id = Column(Integer, primary_key=True)
    student_id = Column(String)
    payment_date = Column(TIMESTAMP)
    plan_type = Column(String)

