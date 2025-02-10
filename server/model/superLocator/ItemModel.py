import json
from typing import Optional, List, Text, Union
from sqlalchemy import Column, Integer, String, DateTime, Boolean
from utility.util import Base
from datetime import datetime
import pytz
from pydantic import BaseModel, field_validator
from sqlalchemy.ext.hybrid import hybrid_property

# Define the Eastern Time zone
eastern = pytz.timezone('America/New_York')

def get_eastern_time():
    return datetime.now(eastern)


class StoreId(BaseModel):
    store: int
    level : Optional[str] = None

class ItemUpdate(BaseModel):
    id: int
    
class ItemReserved(BaseModel):
    id: int
    order_number: str
    loc: Optional[str] = None
    upc: Optional[str] = None 

class ItemLocalisation(BaseModel):
    item: str
    order_number: Optional[str] = None

class ItemAllLocalisation(BaseModel):
    store: int
    
class ItemAllMissing(BaseModel):
    store: int
    
class upc(BaseModel):
    upc: str
    store: Optional[int] = 1

class ItemPicked(BaseModel):
    id: Optional[int] = None
    upc:str
    picked_by:str
    order_number: Optional[str] = None
    loc: Optional[str] = None
    units_to_pick: int = 1 

class FindItem(BaseModel):
    upc: str
    store: Optional[int] = 1

class ByPassItem(BaseModel):
    item: str
    order_number: str
    picked_by: str
    loc: Optional[str] = None

class Item(BaseModel):
    id: int
    store: int
    order_number: str
    item: str
    description: str
    units: int
    state: str = 'false'
    created_at: datetime = get_eastern_time()
    updated_at: datetime = get_eastern_time()
    updated_by: Optional[str] = None 
    loc: str
    is_reserved: bool = False
    is_archived: bool = False
    is_missing: bool = False
    upc: str
    picked_by: Optional[str] = None 
    reserved_by: Optional[str] = None 

    @property
    def store_code(self) -> str:
        return self.loc[0:2]

    @property
    def level(self) -> str:
        return self.loc[2:3]

    @property
    def row(self) -> str:
        return self.loc[3:5]

    @property
    def side(self) -> str:
        return self.loc[5:6]

    @property
    def column(self) -> str:
        return self.loc[6:8]

    @property
    def sides(self) -> str:
        return self.loc[8:9]

    @property
    def bin(self) -> str:
        return self.loc[9:10]
    
class ItemArchived(BaseModel):
    item: str
    loc: str
    is_archived: bool = False

class OrderForm(BaseModel):
    item_name: str
    quantity: int
    store_id: int

class NewOrderRequest(BaseModel):
    store_id: int
    level: int

class ItemReturn(BaseModel):
    item: str
    units: int
    store: int

class BulkReturnRequest(BaseModel):
    items: List[ItemReturn]

class StoreRequest(BaseModel):
    store: int

class ArchiveRequest(BaseModel):
    store: int
    loc:  Optional[str] = None 
    upc: str

# Define the response model
class ReturnResponse(BaseModel):
    id: int
    store: Union[str, int]  # Accept either string or int for store
    item: str
    units: int
    created_at: str
    updated_at: str
    loc: str
    upc: Union[str, List[str]]

    @field_validator('store')
    def validate_store(cls, v):
        # Convert integer store IDs to strings
        return str(v) if isinstance(v, int) else v

    @field_validator('upc')
    def validate_upc(cls, v):
        if isinstance(v, list):
            return v[0] if len(v) == 1 else v
        return v

class Items(Base):
    __tablename__ = "order_info"

    id = Column(Integer, primary_key=True, autoincrement=True)
    store = Column(Integer, nullable=False)
    order_number = Column(String(80))
    item = Column(String(255))
    description = Column(String(255))
    units = Column(Integer)
    state = Column(String(255), default='false')
    created_at = Column(DateTime, default=get_eastern_time)
    updated_at = Column(DateTime, default=get_eastern_time, onupdate=get_eastern_time)
    updated_by = Column(String(255))
    loc = Column(String(64))
    is_reserved = Column(Boolean, default=False) 
    is_archived = Column(Boolean, default=False)
    is_missing = Column(Boolean, default=False)
    upc = Column(String(64))
    picked_by = Column(String(64))
    reserved_by = Column(Integer, nullable=True) 
    
class ItemsReturns(Base):
    __tablename__ = "annual_returns"

    id = Column(Integer, primary_key=True, autoincrement=True)
    store = Column(Integer, nullable=False)
    item = Column(String(255))
    units = Column(Integer)
    created_at = Column(DateTime, default=get_eastern_time)
    updated_at = Column(DateTime, default=get_eastern_time, onupdate=get_eastern_time)
    loc = Column(String(64))
    is_archived = Column(Boolean, default=False)
    _upc = Column('upc', String(255), nullable=False)  # Note the renamed column

    @hybrid_property
    def upc(self):
        """Convert the stored string representation to a list"""
        try:
            return json.loads(self._upc)
        except (json.JSONDecodeError, TypeError):
            # If the UPC is stored as a single value or invalid JSON
            return [self._upc] if self._upc else []

    @upc.setter
    def upc(self, value):
        """Convert list to string representation for storage"""
        if isinstance(value, list):
            self._upc = json.dumps(value)
        elif isinstance(value, str):
            # Try to parse as JSON first in case it's already a JSON string
            try:
                json.loads(value)
                self._upc = value
            except json.JSONDecodeError:
                # If it's a single UPC, store it as a JSON array
                self._upc = json.dumps([value])
        else:
            raise ValueError("UPC must be either a list or string")
        
class ItemsMissing(Base):
    __tablename__ = "is_missing"

    id = Column(Integer, primary_key=True, autoincrement=True)
    store = Column(Integer, nullable=False)
    order_number = Column(String(80))
    item = Column(String(255))
    description = Column(String(255))
    units = Column(Integer)
    state = Column(String(255), default='false')
    created_at = Column(DateTime, default=get_eastern_time)
    updated_at = Column(DateTime, default=get_eastern_time, onupdate=get_eastern_time)
    updated_by = Column(String(255))
    loc = Column(String(64))
    is_reserved = Column(Boolean, default=False) 
    is_archived = Column(Boolean, default=False)
    is_missing = Column(Boolean, default=False)
    upc = Column(String(64))
    picked_by = Column(String(64))
    reserved_by = Column(Integer, nullable=True) 

class ItemsArchived(Base):
    __tablename__ = "is_archived"

    id = Column(Integer, primary_key=True, autoincrement=True)
    store = Column(Integer, nullable=False)
    order_number = Column(String(80))
    item = Column(String(255))
    description = Column(String(255))
    units = Column(Integer)
    state = Column(String(255), default='false')
    created_at = Column(DateTime, default=get_eastern_time)
    updated_at = Column(DateTime, default=get_eastern_time, onupdate=get_eastern_time)
    updated_by = Column(String(255))
    loc = Column(String(64))
    is_reserved = Column(Boolean, default=False) 
    is_archived = Column(Boolean, default=False)
    is_missing = Column(Boolean, default=False)
    upc = Column(String(64))
    picked_by = Column(String(64))
    reserved_by = Column(Integer, nullable=True) 

class ItemsInfo(Base):
    __tablename__ = "inventory"

    id = Column(Integer, primary_key=True, autoincrement=True)
    upc = Column(String)
    item = Column(String)
    description = Column(String)
    package_quantity = Column(Integer)