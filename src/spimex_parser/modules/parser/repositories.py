import datetime
from collections.abc import Iterator
from typing import NamedTuple

import pandas as pd

from spimex_parser.domain import models


class SpimexTradingResultsRepository:
    def __iter__(self) -> Iterator[models.TradingResult]:
        raise NotImplementedError()


class PandasSpimexTradingResultsRepository(SpimexTradingResultsRepository):
    frame: pd.DataFrame
    date: datetime.date


    def __init__(self, frame: pd.DataFrame, date: datetime.date) -> None:
        self.frame = frame
        self.date = date


    def __iter__(self) -> Iterator[models.TradingResult]:
        for row in self.frame.itertuples():
            parsed_row = self._parse_row(row)
            yield parsed_row
    

    def _parse_row(self, row: NamedTuple) -> models.TradingResult:
        current_datetime = datetime.datetime.now()
        exchange_product_id = row[1]
        oil_trading_record = models.TradingResult(
            id=None,
            exchange_product_id=exchange_product_id,
            exchange_product_name=row[2],
            oil_id=exchange_product_id[:4],
            delivery_basis_id=exchange_product_id[4:7],
            delivery_basis_name=row[3],
            delivery_type_id=exchange_product_id[-1],
            volume=int(row[4]),
            total=int(row[5]),
            count=int(row[-1]),
            date=self.date,
            created_on=current_datetime,
            updated_on=current_datetime,
        )
        return oil_trading_record
