from sqlalchemy import Column, String, Boolean, Integer
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel

Base = declarative_base()

class CustomerInfoModel(BaseModel):
    customer_number: str
    customer_name: str
    running_balance: str
    sequence: str
    email_address: str
    ready: bool
    sent: bool
    follow_up: bool

class CustomerInfoResponse(BaseModel):
    customer_number: str
    customer_name: str
    running_balance: str
    sequence: str
    email_address: str
    ready: bool
    sent: bool
    follow_up: bool
    
    class Config:
        orm_mode = True
        from_attributes = True


class CustomerInfo(Base):
    __tablename__ = 'customer_info'
    id = Column(Integer, primary_key=True, autoincrement=True)
    customer_number = Column(String(255), primary_key=True)
    customer_name = Column(String(255))
    running_balance = Column(String(255))
    sequence = Column(String(255))
    email_address = Column(String(255))
    ready = Column(Boolean, default=False)
    sent = Column(Boolean, default=False)
    follow_up = Column(Boolean, default=False)

    def to_dict(self):
        return {
            "customer_number": self.customer_number,
            "customer_name": self.customer_name,
            "running_balance": self.running_balance,
            "sequence": self.sequence,
            "email_address": self.email_address,
            "ready": self.ready,
            "sent": self.sent,
            "follow_up": self.follow_up,
        }