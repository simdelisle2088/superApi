from datetime import datetime
from pydantic import BaseModel
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Boolean,
    func
)
from typing import Optional
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class TransferStoreId(BaseModel):
    store: int

class TransferCustomerId(BaseModel):
    customer: str

class TransferArchiveRequest(BaseModel):
    customer: str
    upc: str

class TransferItemSchema(BaseModel):
    id: Optional[int]
    store: Optional[int]
    tran_to_store: Optional[int]
    customer: Optional[str]
    order_number: Optional[str]
    item: Optional[str]
    description: Optional[str]
    qty_selling_units: Optional[int]
    upc: Optional[str]
    created_at: Optional[datetime]
    is_archived: Optional[bool]

    class Config:
        orm_mode = True
        from_attributes = True 
        
class TransferItems(Base):
    __tablename__ = "pos_transfer_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    store = Column(Integer, nullable=True)
    tran_to_store = Column(Integer, nullable=True)
    customer = Column(String(255), nullable=True)
    order_number = Column(String(255), nullable=True)
    item = Column(String(255), nullable=True)
    description = Column(String(255), nullable=True)
    qty_selling_units = Column(Integer, nullable=True)
    upc = Column(String(64), nullable=True)
    created_at = Column(DateTime, default=func.now())
    is_archived = Column(Boolean, default=False)

