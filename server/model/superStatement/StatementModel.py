from typing import Optional
from sqlalchemy import Column, Integer, String, Boolean
from utility.util import Base
from pydantic import BaseModel
from typing import List


class LoginStatementCreate(BaseModel):
   
    username: str
    password: str


class LoginStatementResponse(BaseModel):
   
    id: int
    username: str
    class Config:
        orm_mode = True


class LoginStatement(Base):
   
    __tablename__ = "statement_users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(255), nullable=False, unique=True)
    password = Column(String(255), nullable=False)


class ClientStatementBillCreate(BaseModel):
 
    client_number: str
    client_name: str
    to: str
    ready: Optional[bool] = False
    sent: Optional[bool] = False
    opened: Optional[bool] = False
    follow_up: Optional[bool] = False
    template: str


class ClientStatementBillResponse(BaseModel):
 
    id: int
    client_number: str
    client_name: str
    email: str
    ready: bool
    sent: bool
    opened: bool
    follow_up: bool
    message: str

    class Config:
        orm_mode = True
        from_attributes = True
    def __repr__(self):
        return f"ClientStatementBillCreate(client_number={self.client_number}, template={self.template})"

class EmailData(BaseModel):
    id: str
    subject: str
    sender: str
    # recipient: str
    # body: str

class CheckEmailResponse(BaseModel):
    emails: List[EmailData]

class ClientStatement(Base):
   
    __tablename__ = "statement_list"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_number = Column(String(255), nullable=False, unique=True)
    client_name = Column(String(255), nullable=False)
    email = Column(String(255), nullable=False)
    ready = Column(Boolean, default=False)
    sent = Column(Boolean, default=False)
    opened = Column(Boolean, default=False)
    follow_up = Column(Boolean, default=False)