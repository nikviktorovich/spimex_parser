import datetime
import os.path
import urllib.parse

import pandas as pd

from spimex_parser.modules.parser import data_table
from spimex_parser.modules.parser import repositories


class SpimexTradingResultsUnitOfWork:
    data: repositories.SpimexTradingResultsRepository

    def __enter__(self) -> 'SpimexTradingResultsUnitOfWork':
        raise NotImplementedError()
    

    def __exit__(self, *args, **kwargs) -> None:
        raise NotImplementedError()


class PandasSpimexTradingResultsUnitOfWork(SpimexTradingResultsUnitOfWork):
    def __init__(self, oil_data_path: str) -> None:
        self.oil_data_path = oil_data_path


    def __enter__(self) -> SpimexTradingResultsUnitOfWork:
        frame = pd.read_excel(self.oil_data_path, na_values=['-'])
        date = self._parse_date_from_path(self.oil_data_path)
        trading_results_table = data_table.PandasTradingResultsDataTable(frame, date)
        self.data = repositories.TableSpimexTradingResultsRepository(
            trading_results_table,
            date,
        )
        return self
    

    def _parse_date_from_path(self, path: str) -> datetime.date:
        file_name = self._parse_file_name_from_path(path)
        file_date_time = self._parse_datetime_from_file_name(file_name)
        return file_date_time.date()
    

    def _parse_file_name_from_path(self, path: str) -> str:
        relative_path = urllib.parse.urlparse(path).path
        file_name = os.path.split(relative_path)[-1]
        return file_name
    

    def _parse_datetime_from_file_name(self, file_name: str) -> datetime.datetime:
        DATETIME_FORMAT = '%Y%m%d%H%M%S'
        file_name_without_extension = os.path.splitext(file_name)[0]
        timestamp = file_name_without_extension.split('_')[-1]
        return datetime.datetime.strptime(timestamp, DATETIME_FORMAT)
    

    def __exit__(self, *args, **kwargs) -> None:
        return
