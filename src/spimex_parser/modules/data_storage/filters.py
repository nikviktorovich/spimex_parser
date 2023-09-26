import dataclasses
import datetime
from typing import Optional


@dataclasses.dataclass
class TradingResultFilter:
    oil_id: Optional[str] = None
    delivery_type_id: Optional[str] = None
    delivery_basis_id: Optional[str] = None
    start_date: Optional[datetime.date] = None
    end_date: Optional[datetime.date] = None
