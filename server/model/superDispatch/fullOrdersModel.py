# from pyrate_limiter import Optional
# from sqlalchemy import Column, Integer, String, Text, DateTime, Double, Boolean
# from utility.util import Base, DEFAULT_DATETIME, DEFAULT_IMAGE_FILENAME
# from pydantic import BaseModel
# from datetime import datetime
# from zoneinfo import ZoneInfo

# eastern = ZoneInfo('America/New_York')
# def current_time_eastern():
#     return datetime.now(eastern)

# class AvgDeliveryTime(BaseModel):
#     store_id: int
    
# class FullOrders(Base):
#     __tablename__ = "full_orders"

#     id = Column(Integer, primary_key=True, autoincrement=True)
#     created_at = Column(DateTime, default=current_time_eastern, nullable=False)
#     updated_at = Column(DateTime, default=current_time_eastern, onupdate=current_time_eastern, nullable=False)
#     clerk_name = Column(String(255), nullable=True)
#     order_info = Column(Text, nullable=True)
#     phone_number = Column(String(64), nullable=True)
#     client_name = Column(String(255), nullable=True)
#     order_number = Column(String(45), nullable=True)
#     store = Column(Integer, nullable=True)
#     customer = Column(String(255), nullable=True)
#     pickers = Column(Boolean, default=False, nullable=True)
#     dispatch = Column(Boolean, default=False, nullable=True)
#     drivers = Column(Boolean, default=False, nullable=True)
#     address1 = Column(String(255), nullable=True)
#     address2 = Column(String(255), nullable=True)
#     address3 = Column(String(255), nullable=True)
#     ship_addr1 = Column(String(255), nullable=True)
#     ship_addr2 = Column(String(255), nullable=True)
#     ship_addr3 = Column(String(255), nullable=True)
#     price = Column(String(255), nullable=True)
