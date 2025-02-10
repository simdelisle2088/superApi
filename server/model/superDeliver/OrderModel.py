from sqlalchemy import Column, Integer, String, Boolean, Text, DateTime
from utility.util import Base
from datetime import datetime


class Order(Base):
    __tablename__ = "full_orders"
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_number = Column(String(80), nullable=False)
    store = Column(String(255), nullable=False)
    customer = Column(String(255), nullable=False)
    client_name = Column(String(255))
    phone_number = Column(String(64))
    order_info = Column(Text)
    pickers = Column(Boolean, nullable=False)
    dispatch = Column(Boolean, nullable=False)
    drivers = Column(Boolean, nullable=False)
    address1 = Column(String(255))
    address2 = Column(String(255))
    address3 = Column(String(255))
    ship_addr1 = Column(String(255))
    ship_addr2 = Column(String(255))
    ship_addr3 = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )
    price = Column(String(255))
    job = Column(Integer, nullable=True)
