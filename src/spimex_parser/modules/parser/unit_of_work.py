import datetime
import os.path
import urllib.parse

import pandas as pd

from spimex_parser.modules.parser import repositories


class SpimexTradingResultsUnitOfWork:
    data: repositories.SpimexTradingResultsRepository

    def __enter__(self) -> 'SpimexTradingResultsUnitOfWork':
        raise NotImplementedError()
    

    def __exit__(self, *args, **kwargs) -> None:
        raise NotImplementedError()


class PandasSpimexTradingResultsUnitOfWork(SpimexTradingResultsUnitOfWork):
    oil_data_path: str


    def __init__(self, oil_data_path: str) -> None:
        self.oil_data_path = oil_data_path


    def __enter__(self) -> SpimexTradingResultsUnitOfWork:
        frame = pd.read_excel(self.oil_data_path, na_values=['-'])
        clean_frame = self._extract_table(frame)
        date = self._parse_date_from_path(self.oil_data_path)
        self.data = repositories.PandasSpimexTradingResultsRepository(
            clean_frame,
            date,
        )
        return self
    

    def _extract_table(self, frame: pd.DataFrame) -> pd.DataFrame:
        frame = self._skip_empty_column(frame)
        frame = self._extract_table_rows(frame)
        frame = self._drop_nan_contracts(frame)
        return frame
    

    def _skip_empty_column(self, frame: pd.DataFrame) -> pd.DataFrame:
        return frame.iloc[:, 1:]
    

    def _extract_table_rows(self, frame: pd.DataFrame) -> pd.DataFrame:
        SUMMARY_ROWS_COUNT = 2
        table_start_index = self._find_table_start_index(frame)
        return frame.iloc[table_start_index:-SUMMARY_ROWS_COUNT]
    

    def _find_table_start_index(self, frame: pd.DataFrame) -> int:
        HEADERS_OFFSET = 2
        table_name = 'Единица измерения: Метрическая тонна'
        table_name_search_result = frame.iloc[:, 0] == table_name
        return table_name_search_result.argmax() + 1 + HEADERS_OFFSET # type: ignore
    

    def _drop_nan_contracts(self, frame: pd.DataFrame) -> pd.DataFrame:
        return frame[frame.iloc[:, -1].notna()]
    

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
