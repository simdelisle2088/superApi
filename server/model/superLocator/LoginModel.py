from typing import Optional
from sqlalchemy import Column, Integer, String, DateTime
from model.superLocator.ItemModel import get_eastern_time
from utility.util import Base
from pydantic import BaseModel
from datetime import datetime
from sqlalchemy.orm import relationship

class DeviceInfo(BaseModel):
    """Model for device information received from client"""
    deviceId: str
    deviceType: str
    model: Optional[str]
    manufacturer: Optional[str]
    platform: str
    platformVersion: Optional[str]
    fingerprint: Optional[str]
    isPhysicalDevice: Optional[bool]

class LocatorLogin(BaseModel):
    username: str
    password: str
    deviceInfo: DeviceInfo

class LocatorCreate(BaseModel):
    username: str
    password: str
    store: int

class PickerId(BaseModel):
    picker_id: int

class UserId(BaseModel):
   id: int

class UsersLocator(Base):
    __tablename__ = "locator_users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(255), nullable=False, unique=True)
    password = Column(String(255), nullable=False)
    store = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=get_eastern_time(), nullable=False)

    devices = relationship("UserDevice", back_populates="user", cascade="all, delete-orphan")