from pydantic import BaseModel
from typing import Optional

# SQLAlchemy model
from sqlalchemy import Column, Integer, String, DECIMAL, create_engine
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Commis(BaseModel):
    store: int
    counterman: Optional[str]
    net_sales: Optional[float]
    net_sales_MTD: Optional[float]
    st_hubert_summ: Optional[float]
    st_jean_summ: Optional[float]
    chateau_summ: Optional[float]

    class Config:
        orm_mode = True
        from_attributes = True

class CommisStats(Base):
    __tablename__ = 'commis_stats'

    id = Column(Integer, primary_key=True, autoincrement=True)
    store = Column(Integer, nullable=False)
    counterman = Column(String(255), nullable=True)
    net_sales = Column(DECIMAL(10, 2), nullable=True)
    net_sales_MTD = Column(DECIMAL(10, 2), nullable=True)
    st_hubert_summ = Column(DECIMAL(10, 2), nullable=True)
    st_jean_summ = Column(DECIMAL(10, 2), nullable=True)
    chateau_summ = Column(DECIMAL(10, 2), nullable=True)