import datetime
from typing import List
from typing import NamedTuple

import pandas as pd

from spimex_parser.domain import models


class TradingResultsDataTable:
    """Таблица данных о результатах торгов"""
    def list_results(self) -> List[models.TradingResult]:
        """Возвращает список результатов торгов"""
        raise NotImplementedError()


class PandasTradingResultsDataTable(TradingResultsDataTable):
    """Таблица данных о результатах торгов
    
    Таблица данных о результатах торгов, служащая адаптером для данных,
    импортированных в виде pandas таблицы
    """
    frame: pd.DataFrame
    date: datetime.date


    def __init__(self, frame: pd.DataFrame, date: datetime.date) -> None:
        self.frame = self._extract_table(frame)
        self.date = date
    

    def _extract_table(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Извлекает таблицу из оригинального формата данных
        
        Извлекает таблицу с единицами измерения в метрических тоннах,
        попутно убирая все лишнее, включая строки с пустым числом контрактов
        """
        frame = self._skip_empty_column(frame)
        frame = self._extract_table_rows(frame)
        frame = self._drop_nan_contracts(frame)
        return frame
    

    def _skip_empty_column(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Избавляется от пустого столбца слева"""
        return frame.iloc[:, 1:]
    

    def _extract_table_rows(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Извлекает строки нужной таблицы"""
        table_start_index = self._find_table_start_index(frame)
        top_stripped_frame = frame.iloc[table_start_index:]

        table_end_index = self._find_table_end_index(top_stripped_frame)
        stripped_frame = top_stripped_frame[:table_end_index]

        return stripped_frame
    

    def _find_table_start_index(self, frame: pd.DataFrame) -> int:
        """Извлекает индекс строки начала нужной таблицы"""
        HEADERS_OFFSET = 2
        TABLE_NAME = 'Единица измерения: Метрическая тонна'
        table_name_search_result = frame.iloc[:, 0] == TABLE_NAME
        table_name_row_index = int(table_name_search_result.argmax())
        return table_name_row_index + 1 + HEADERS_OFFSET
    

    def _find_table_end_index(self, frame: pd.DataFrame) -> int:
        """Извлекает индекс строки конца нужной таблицы"""
        SUMMARY_CELL_NAME = 'Итого:'
        summary_row_search_result = frame.iloc[:, 0] == SUMMARY_CELL_NAME
        summary_row_row_index = int(summary_row_search_result.argmax())
        return summary_row_row_index
    

    def _drop_nan_contracts(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Избавляется от строк с пустым числом контрактов"""
        CONTRACTS_COL_INDEX = -1
        non_empty_contracts_mask = frame.iloc[:, CONTRACTS_COL_INDEX].notna()
        return frame[non_empty_contracts_mask]


    def list_results(self) -> List[models.TradingResult]:
        """Возвращает список результатов торгов"""
        results: List[models.TradingResult] = []

        for row in self.frame.itertuples():
            trading_result = self._parse_row(row)
            results.append(trading_result)
        
        return results
    

    def _parse_row(self, row: NamedTuple) -> models.TradingResult:
        """Приводит строку столбца данных в необходимый вид"""
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
