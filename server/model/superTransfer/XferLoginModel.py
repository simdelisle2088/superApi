from sqlalchemy import Column, Integer, String, DateTime
from utility.util import Base
from pydantic import BaseModel
from datetime import datetime

class XferLogin(BaseModel):
    username: str
    password: str

class XferCreate(BaseModel):
    username: str
    password: str
    store: int

class XferUserId(BaseModel):
   id: int

class UsersXfer(Base):
    __tablename__ = "xfer_users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(255), nullable=False, unique=True)
    password = Column(String(255), nullable=False)
    store = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
