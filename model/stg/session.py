from model.base import Base
from sqlalchemy import Column, BigInteger, Integer, String, TIMESTAMP

class Session(Base):
    __tablename__ = 'sessions'

    # id = Column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    id = Column(Integer, primary_key=True)
    student_id = Column(String)
    session_start_time = Column(TIMESTAMP)
    student_client = Column(String)

