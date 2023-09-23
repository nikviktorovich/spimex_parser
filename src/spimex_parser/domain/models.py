import dataclasses
import datetime
import uuid
from typing import Optional


@dataclasses.dataclass
class TradingResult:
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
    id: Optional[uuid.UUID] = None
