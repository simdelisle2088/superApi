from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel
from pyrate_limiter import Optional

Base = declarative_base()

# SQLAlchemy Model
class PosArcDHead(Base):
    __tablename__ = 'pos_arc_d_head'

    id = Column(Integer, primary_key=True, autoincrement=True)
    customer = Column(String(255))
    customer_number = Column(String(255))
    job = Column(String(55))
    address = Column(String(255))
    postal_code = Column(String(45), nullable=False)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)

# Pydantic Model
class PosArcDHeadBaseModel(BaseModel):
    id: int
    customer: str | None = None
    customer_number: str | None = None
    job: str | None = None
    address: str | None = None
    postal_code: Optional[str]
    latitude: float
    longitude: float

    class Config:
        orm_mode = True

class ClientAddressResponse(BaseModel):
    customer: Optional[str]
    customer_number: Optional[str]
    job: Optional[str]
    address: Optional[str]
    postal_code: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]

    class Config:
        orm_mode = True
        
class OrderRequest(BaseModel):
    order_number: str
    job: int

class UpdateClientRequest(BaseModel):
    customer: str
    customer_number: str
    job: str
    address: str
    postal_code: str
    latitude: float
    longitude: float

    class Config:
        orm_mode = True