from typing import List
from pyrate_limiter import Optional
from sqlalchemy import Column, Integer, String, DateTime, Boolean
from utility.util import Base
from pydantic import BaseModel, Field
from datetime import datetime

class DeliveryTimeRequest(BaseModel):
    store_id: int
    driver_id: int
    
class DriverId(BaseModel):
    driver_id: int
    
class DriverIdRoute(BaseModel):
    driver_id: int
    route_number: str

class StoreId(BaseModel):
    store: str
    search: Optional[List[str]] = []

    class Config:
        from_attributes = True  

class DriverLogin(BaseModel):
    username: str
    password: str

class DriverCreateOrUpdate(BaseModel):
    driver_id: int = Field(
        default=None,
    )
    username: str
    password: str
    store: int

class Driver(Base):
    __tablename__ = "drivers"
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(255), nullable=False, unique=True)
    password = Column(String(255), nullable=False)
    store = Column(Integer, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
