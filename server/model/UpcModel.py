from sqlalchemy import Column, Integer, String, DateTime
from utility.util import Base


class Upc(Base):
    __tablename__ = "upcs"
    part_number = Column(String, primary_key=True)
    upc = Column(String)
    quantity = Column(Integer)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
