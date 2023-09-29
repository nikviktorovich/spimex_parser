import datetime

import pydantic


class TradingDateBase(pydantic.BaseModel):
    date: datetime.date


class TradingDateRead(TradingDateBase):
    pass
