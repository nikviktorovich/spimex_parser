import datetime
import io
import os.path
import urllib.parse

import aiohttp
import pandas as pd

from spimex_parser.modules.parser import data_table
from spimex_parser.modules.parser.asyncio import repositories


class AsyncSpimexTradingResultsUnitOfWork:
    """Асинхронная единица работы с данными о результатах торгов со Spimex"""
    data: repositories.AsyncSpimexTradingResultsRepository

    async def __aenter__(self) -> 'AsyncSpimexTradingResultsUnitOfWork':
        raise NotImplementedError()
    

    async def __aexit__(self, *args, **kwargs) -> None:
        raise NotImplementedError()


class AsyncPandasSpimexTradingResultsUnitOfWork(AsyncSpimexTradingResultsUnitOfWork):
    """Асинхронная единица работы с данными о результатах торгов со Spimex
    
    Асинхронная единица работы с данными о результатах торгов со Spimex,
    полученных в виде excel-таблицы
    """
    client: aiohttp.ClientSession


    def __init__(self, oil_data_path: str, client: aiohttp.ClientSession) -> None:
        self.oil_data_path = oil_data_path
        self.client = client


    async def __aenter__(self) -> AsyncSpimexTradingResultsUnitOfWork:
        frame = await self._read_excel(self.oil_data_path)
        date = self._parse_date_from_path(self.oil_data_path)
        trading_results_table = data_table.PandasTradingResultsDataTable(frame, date)
        self.data = repositories.AsyncTableSpimexTradingResultsRepository(
            trading_results_table,
            date,
        )
        return self


    async def _read_excel(self, url: str) -> pd.DataFrame:
        """Возвращает асинхронно загруженный и прочитанный excel-файл"""
        async with self.client.get(url) as resp:
            file_bytes = await resp.read()
            file_bytes_stream = io.BytesIO(file_bytes)
            frame = pd.read_excel(file_bytes_stream, na_values=['-'])
            return frame
    

    def _parse_date_from_path(self, path: str) -> datetime.date:
        """Извлекает дату из пути к файлу данных"""
        file_name = self._parse_file_name_from_path(path)
        file_date_time = self._parse_datetime_from_file_name(file_name)
        return file_date_time.date()
    

    def _parse_file_name_from_path(self, path: str) -> str:
        """Извлекает название файла из пути"""
        relative_path = urllib.parse.urlparse(path).path
        file_name = os.path.split(relative_path)[-1]
        return file_name
    

    def _parse_datetime_from_file_name(self, file_name: str) -> datetime.datetime:
        """Извлекает дату из названия файла"""
        DATETIME_FORMAT = '%Y%m%d%H%M%S'
        file_name_without_extension = os.path.splitext(file_name)[0]
        timestamp = file_name_without_extension.split('_')[-1]
        return datetime.datetime.strptime(timestamp, DATETIME_FORMAT)
    

    async def __aexit__(self, *args, **kwargs) -> None:
        return
