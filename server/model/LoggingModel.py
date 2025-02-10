from sqlalchemy import Column, Integer, String, DateTime, Double
from pydantic import BaseModel
from model.superLocator.ItemModel import get_eastern_time
from utility.util import Base
from datetime import datetime


class TelemetryRequest(BaseModel):
    order_number: str
    store: int
    latitude: float
    longitude: float

class RouteRequestData(BaseModel):
    origin: str
    destination: str
    departureTime: str = None

class RouteResponseData(BaseModel):
    routes: list

class Telemetry(Base):
    __tablename__ = "telemetry"
    id = Column(Integer, primary_key=True, autoincrement=True)
    driver_id = Column(Integer, nullable=False)
    order_number = Column(String(32), nullable=False)
    store = Column(Integer, nullable=False)
    latitude = Column(Double, nullable=False)
    longitude = Column(Double, nullable=False)
    created_at = Column(DateTime(timezone=True), default=get_eastern_time, nullable=False)

class SkippedPart(Base):
    __tablename__ = "skipped_parts"
    id = Column(Integer, primary_key=True, autoincrement=True)
    driver_id = Column(Integer, nullable=False)
    order_number = Column(String(32), nullable=False)
    store = Column(Integer, nullable=False)
    part_number = Column(String(32), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
