from typing import Dict
from sqlalchemy import Column, Integer, String
from utility.util import Base
from pydantic import BaseModel


# Model for Store coordinates
class StoreCoordinates(BaseModel):
    coordinates: Dict[int, str]

class StoreCoords(Base):
    __tablename__ = 'store_coords'
    id = Column(Integer, primary_key=True)
    coordinates = Column(String)

