from pyrate_limiter import Optional
from sqlalchemy import Column, Integer, String, DateTime, Boolean
from model.superLocator.ItemModel import get_eastern_time
from utility.util import Base
from pydantic import BaseModel, ConfigDict, validator
from sqlalchemy.orm import class_mapper

class Inv(BaseModel):
    upc:str
    store:int
    part_code:str
    line_number:str
    package_quantity:int
    description:str
    part_number:str
    quantity:float
    item:str
    

class InvScan(BaseModel):
    upc:list
    name:list
    updated_by:str
    loc: str
    archive: bool
    quantity: int = 1 

class Location(BaseModel):
    store: str
    level: str
    row: str
    side: str
    column: str
    shelf: str
    bin: str
    full_location: str
    created_by: Optional[str] = None
    updated_by: Optional[str] = None 
    created_at: Optional[str] = None 
    updated_at: Optional[str] = None 

class Localisation(BaseModel):
    storeId: int

class InvLocationBase(BaseModel):
    upc: str
    name: str
    store: str
    level: Optional[str] = None
    row: Optional[str] = None
    side: Optional[str] = None
    column: Optional[str] = None
    shelf: Optional[str] = None
    bin: Optional[str] = None
    full_location: str
    updated_by: Optional[str] = None
    updated_at: Optional[DateTime] = None
    created_by: Optional[str] = None
    created_at: Optional[DateTime] = None
    is_archived: Optional[bool] = False

    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)

class InvLocationArchiveRequest(BaseModel):
    upc: str
    full_location: str

class UPCRequest(BaseModel):
    upc: str

class StoreIdRequest(BaseModel):
    store: int

class FullLocationRequest(BaseModel):
    full_location: str
    
class ItemResponse(BaseModel):
    name: str
    upc: str
    count: int

class LocationDeleteResponse(BaseModel):
    locations_deleted: int
    message: str


class LocationDeleteRequest(BaseModel):
    store: str
    level: str
    row: str
    side: str
      
class InvLocations(Base):
    __tablename__ = "locations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    upc = Column(String)
    name = Column(String)
    store = Column(String)
    level = Column(String)
    row = Column(String)
    side = Column(String)
    column = Column(String)
    shelf = Column(String)
    bin = Column(String)
    full_location = Column(String)
    updated_by = Column(String)
    updated_at = Column(DateTime, default=get_eastern_time, onupdate=get_eastern_time)
    created_by = Column(String)
    created_at = Column(DateTime, default=get_eastern_time)
    is_archived = Column(Boolean, default=False)
    
    def to_dict(self):
        # Use class_mapper to get all columns of the model
        columns = class_mapper(self.__class__).columns
        return {column.name: getattr(self, column.name) for column in columns}

class palletLocations(Base):
    __tablename__ = "loc_pallet"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    store =  Column(Integer)
    item =  Column(String)
    loc =  Column(String)