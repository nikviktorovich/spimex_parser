import datetime
from typing import List

from spimex_parser.domain import models
from spimex_parser.modules.parser import data_table


class SpimexTradingResultsRepository:
    def list(self) -> List[models.TradingResult]:
        raise NotImplementedError()


class TableSpimexTradingResultsRepository(SpimexTradingResultsRepository):
    results_data_table: data_table.TradingResultsDataTable
    date: datetime.date


    def __init__(
        self,
        results_data_table: data_table.TradingResultsDataTable,
        date: datetime.date,
    ) -> None:
        self.results_data_table = results_data_table
        self.date = date


    def list(self) -> List[models.TradingResult]:
        return self.results_data_table.list_results()
