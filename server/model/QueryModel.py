import datetime
from pydantic import BaseModel
from typing import Optional


class QueryParams(BaseModel):
    count: Optional[int] = None
    offset: Optional[int] = None
    dateFrom: Optional[datetime.date] = None
    dateTo: Optional[datetime.date] = None
    contain: Optional[str] = None

    model_config = {
        'arbitrary_types_allowed': True
    }