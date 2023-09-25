import datetime
from typing import List

from spimex_parser.domain import models
from spimex_parser.modules.parser import data_table


class AsyncSpimexTradingResultsRepository:
    """Асинхронный репозиторий данных о результатах торгов со Spimex"""
    async def list(self) -> List[models.TradingResult]:
        """Возвращает список данных о результатах торгов"""
        raise NotImplementedError()


class AsyncTableSpimexTradingResultsRepository(AsyncSpimexTradingResultsRepository):
    """Репозиторий данных о результатах торгов со Spimex
    
    Синхронный репозиторий данных с асинхронным API о результатах торгов
    со Spimex, полученных в виде TradingResultsDataTable класса
    """
    results_data_table: data_table.TradingResultsDataTable
    date: datetime.date


    def __init__(
        self,
        results_data_table: data_table.TradingResultsDataTable,
        date: datetime.date,
    ) -> None:
        self.results_data_table = results_data_table
        self.date = date


    async def list(self) -> List[models.TradingResult]:
        """Возвращает список данных о результатах торгов"""
        return self.results_data_table.list_results()
