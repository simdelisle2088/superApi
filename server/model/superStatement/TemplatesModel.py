from sqlalchemy import Column, Integer, String, Text
from utility.util import Base
from pydantic import BaseModel


class HTMLContent(BaseModel):
    fileName: str
    html: str
    

class HTMLTemplate(Base):
    __tablename__ = 'html_templates'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    content = Column(Text, nullable=False)