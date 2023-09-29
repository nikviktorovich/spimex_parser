import datetime
import uuid
from typing import Optional

import pydantic


class DynamicsBase(pydantic.BaseModel):
    exchange_product_id: str
    exchange_product_name: str
    oil_id: str
    delivery_basis_id: str
    delivery_basis_name: str
    delivery_type_id: str
    volume: int
    total: int
    count: int
    date: datetime.date
    created_on: datetime.datetime
    updated_on: datetime.datetime


class DynamicsRead(DynamicsBase):
    model_config = pydantic.ConfigDict(from_attributes=True)

    id: uuid.UUID


class DynamicsFilter(pydantic.BaseModel):
    oil_id: Optional[str] = None
    delivery_type_id: Optional[str] = None
    delivery_basis_id: Optional[str] = None
    start_date: Optional[datetime.date] = None
    end_date: Optional[datetime.date] = None
