from typing import Dict, List
from pyrate_limiter import Optional
from sqlalchemy import Column, Integer, String, Text, DateTime, Double, Boolean
from utility.util import Base, DEFAULT_DATETIME, DEFAULT_IMAGE_FILENAME
from pydantic import BaseModel, RootModel
from datetime import date, datetime
from zoneinfo import ZoneInfo

eastern = ZoneInfo('America/New_York')
def current_time_eastern():
    return datetime.now(eastern)

class InvoiceCode(BaseModel):
    invoice_code: str
    client_number: Optional[str] = None 

    @property
    def cleaned_code(self) -> str:
        return self.invoice_code.replace(" ", "0")

    @property
    def customer(self) -> str:
        return self.cleaned_code[2:8]

    @property
    def order_number(self) -> str:
        return self.cleaned_code[11:17]

    @property
    def store(self) -> str:
        return self.cleaned_code[19:20]
    
    def set_client_number(self):
        if not self.client_number:
            self.client_number = self.customer

class InvoiceInfo(BaseModel):
    orderNumber: str
    store: int

class OrderReorderRequest(BaseModel):
    order_number: str
    new_index: int

class ScannedPart(BaseModel):
    orderNumber: str
    store: int
    partCode: str


class ScannedBatch(BaseModel):
    orderNumber: str
    store: int
    partNumber: str

class DriverOrderNumber(BaseModel): 
    order_number: int

class ReceivedBy(BaseModel): 
    order_number: str
    received_by: str

class StartRouteRequest(BaseModel):
    route: str

class ClientOrders(BaseModel):
    store: int
    search: Optional[str] = None 

class AllOrdersByStore(BaseModel):
    storeId: int

class OrderNumberRequest(BaseModel):
    order_numbers: List[str]

class TimeRangeStats(BaseModel):
    min_1_20: int
    min_21_40: int
    min_41_60: int
    min_60_90: int 
    min_90_plus: int 
    last_30_days: int
    last_60_days: int
    last_90_days: int
    last_120_days: int
    avg_picking_time: int

class StoreDeliveryStats(RootModel):
    root: Dict[str, TimeRangeStats] 
    
class DeliveryCountRequest(BaseModel):
    storeId: int
    startDate: date
    endDate: date 

class TimeRangeFilteredStats(BaseModel):
    min_1_20: int
    min_21_40: int
    min_41_60: int
    min_60_90: int 
    min_90_plus: int  
    avg_picking_time: int = 0
    unique_drivers: int = 0 
    
DeliveryStatsResponse = Dict[str, TimeRangeFilteredStats]

class DriverStats(BaseModel):
    driver_name: str
    total_deliveries: int
    last_30_days: int
    last_60_days: int

    class Config:
        from_attributes = True

class StoreRequest(BaseModel):
    store: int

class SearchOrderRequest(BaseModel):
    search_query: str
    store: str
    
class DriverOrderResponse(BaseModel):
    id: int
    tracking_number: str
    order_number: str
    merged_order_numbers: str
    store: int
    customer: str
    order_info: str
    client_name: str
    phone_number: str
    latitude: float
    longitude: float
    address: str
    ship_addr: str
    driver_id: int
    photo_filename: str
    is_arrived: bool
    is_delivered: bool
    created_at: datetime
    arrived_at: datetime
    delivered_at: datetime
    updated_at: datetime
    driver_name: str
    order_index: Optional[int]
    route: str
    route_started: bool
    received_by: Optional[str] 
    price: str
    job: int

    class Config:
        orm_mode = True

class DriverOrder(Base):
    __tablename__ = "drivers_orders"
    id = Column(Integer, primary_key=True, autoincrement=True)
    tracking_number = Column(String(32), nullable=False)
    order_number = Column(String(32), nullable=False)
    merged_order_numbers = Column(Text, nullable=False)
    store = Column(Integer, nullable=False)
    customer = Column(String(32), nullable=False)
    order_info = Column(Text, nullable=False)
    client_name = Column(String(64), nullable=False)
    phone_number = Column(String(32), nullable=False)
    latitude = Column(Double, nullable=False)
    longitude = Column(Double, nullable=False)
    address = Column(String(128), nullable=False)
    ship_addr = Column(String(128), nullable=False)
    driver_id = Column(Integer, nullable=False)
    photo_filename = Column(
        String(384),
        default=DEFAULT_IMAGE_FILENAME,
        nullable=False,
    )
    is_arrived = Column(Boolean, default=False, nullable=False)
    is_delivered = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    arrived_at = Column(DateTime, default=DEFAULT_DATETIME, nullable=False)
    delivered_at = Column(DateTime, default=DEFAULT_DATETIME, nullable=False)
    updated_at = Column(DateTime, default=current_time_eastern, onupdate=current_time_eastern, nullable=False)
    driver_name = Column(String(32), nullable=False)
    order_index = Column(Integer) 
    route = Column(String(32), nullable=False)
    route_started = Column(Boolean, default=False, nullable=False)
    received_by = Column(String(64), nullable=False)
    price = Column(String(255), nullable=False)
    job = Column(Integer, nullable=True)