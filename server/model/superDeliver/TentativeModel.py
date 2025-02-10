from zoneinfo import ZoneInfo
from sqlalchemy import Column, Integer, String, Text, DateTime, Double, Boolean
from utility.util import Base, DEFAULT_DATETIME
from pydantic import BaseModel
from datetime import datetime

eastern = ZoneInfo('America/New_York')
def current_time_eastern():
    return datetime.now(eastern)

class TentativeOrder(BaseModel):
    orderNumber: str
    store: int

class DriverTentativeOrder(Base):
    __tablename__ = "tentative_orders"
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_number = Column(String(32), nullable=False)
    store = Column(Integer, nullable=False)
    customer = Column(String(32), nullable=False)
    order_info = Column(Text, nullable=False)
    client_name = Column(String(64), nullable=False)
    phone_number = Column(String(32), nullable=False)
    latitude = Column(Double, nullable=False)
    longitude = Column(Double, nullable=False)
    address = Column(String(128), nullable=False)
    driver_id = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    tentative_at = Column(DateTime, default=DEFAULT_DATETIME, nullable=False)
    updated_at = Column(DateTime, default=current_time_eastern, onupdate=current_time_eastern, nullable=False)
    driver_name = Column(String(32), nullable=False)
    route = Column(String(32), nullable=False)
    route_started = Column(Boolean, default=False, nullable=False)
    received_by = Column(String(64), nullable=False)
    price = Column(String(255), nullable=False)