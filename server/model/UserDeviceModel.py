# models/database/user_device.py
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, and_, select
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from model.superLocator.ItemModel import get_eastern_time
from model.superLocator.LoginModel import DeviceInfo
from utility.util import Base

class DeviceBase(BaseModel):
    """Base schema for device information"""
    device_id: str
    device_type: str
    device_model: Optional[str] = None
    manufacturer: Optional[str] = None
    platform: str
    platform_version: Optional[str] = None
    fingerprint: Optional[str] = None

    # Configure Pydantic to handle complex types
    model_config = ConfigDict(
        from_attributes=True,  # Allows conversion from ORM models
        arbitrary_types_allowed=True  # Allows datetime and other complex types
    )

class DeviceCreate(DeviceBase):
    """Schema for creating a new device"""
    user_id: int

class DeviceInDB(DeviceBase):
    """Schema for device information as stored in database"""
    id: int
    user_id: int
    last_login: datetime
    is_active: bool

    # Configure Pydantic to handle ORM models and complex types
    model_config = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=True
    )

class UserDevice(Base):
    """SQLAlchemy model for user devices in the database"""
    __tablename__ = "locator_user_devices"
    
    # Define database columns with appropriate constraints
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("locator_users.id", ondelete="CASCADE"), nullable=False)
    device_id = Column(String(255), nullable=False, index=True)  # Added index for better query performance
    device_type = Column(String(50))
    device_model = Column(String(100))
    manufacturer = Column(String(100))
    platform = Column(String(50))
    platform_version = Column(String(50))
    last_login = Column(DateTime, default=get_eastern_time, nullable=False)  # Note: removed parentheses
    is_active = Column(Boolean, default=True, nullable=False)
    fingerprint = Column(String(255))
    
    # Relationship with UsersLocator model
    user = relationship("UsersLocator", back_populates="devices")

    @classmethod
    async def find_user_device(cls, db, user_id: int, device_id: str):
        """
        Find an existing device for a user.
        Returns the device if found and active, None otherwise.
        """
        result = await db.execute(
            select(cls).where(
                and_(
                    cls.user_id == user_id,
                    cls.device_id == device_id,
                    cls.is_active == True
                )
            )
        )
        return result.scalars().first()

    @classmethod
    async def create_from_device_info(cls, db, user_id: int, device_info: DeviceInfo):
        """
        Create a new device record from DeviceInfo.
        Maps the device information from the API to our database model.
        """
        new_device = cls(
            user_id=user_id,
            device_id=device_info.deviceId,
            device_type=device_info.deviceType,
            device_model=device_info.model,
            manufacturer=device_info.manufacturer,
            platform=device_info.platform,
            platform_version=device_info.platformVersion,
            fingerprint=device_info.fingerprint,
            last_login=get_eastern_time()
        )
        db.add(new_device)
        await db.flush()
        return new_device

    async def update_last_login(self, db):
        """Update the last login time for this device"""
        self.last_login = get_eastern_time()
        await db.flush()

    def to_dict(self):
        """
        Convert the model instance to a dictionary.
        Useful for serialization and API responses.
        """
        return {
            "id": self.id,
            "user_id": self.user_id,
            "device_id": self.device_id,
            "device_type": self.device_type,
            "device_model": self.device_model,
            "manufacturer": self.manufacturer,
            "platform": self.platform,
            "platform_version": self.platform_version,
            "last_login": self.last_login,
            "is_active": self.is_active,
            "fingerprint": self.fingerprint
        }