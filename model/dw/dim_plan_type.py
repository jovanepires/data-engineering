from model.base import Base
from sqlalchemy import Column, Integer, String

class DimPlanType(Base):
    __tablename__ = 'dim_plan_type'

    plan_type_id = Column(Integer, primary_key=True)
    name = Column(String)

