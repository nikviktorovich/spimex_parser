import uuid
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Set

from spimex_parser.domain import models
from spimex_parser.modules.data_storage import filters
from spimex_parser.modules.data_storage.asyncio import repositories


class AsyncFakeTradingResultsRepository(repositories.AsyncTradingResultsRepository):
    _data: Dict[uuid.UUID, models.TradingResult]
    _uncommitted_data: List[models.TradingResult]


    def __init__(self, data: List[models.TradingResult]) -> None:
        self._data = {result.id: result for result in data} # type: ignore
        self._uncommitted_data = []
    

    async def get(self, trading_result_id: uuid.UUID) -> Optional[models.TradingResult]:
        """Возвращает данных о результате торгов с указанным id
        
        Возвращает данных о результате торгов с указанным id или None в случае,
        если записи с таким id не существует
        """
        return self._data.get(trading_result_id)
    

    def add(self, trading_result: models.TradingResult) -> models.TradingResult:
        """Добавляет данные о результатах сделки в репозиторий"""
        if trading_result.id is not None:
            raise ValueError('You are not allowed to specify record ID manually')
        
        trading_result.id = uuid.uuid4()
        self._uncommitted_data.append(trading_result)
        return trading_result
    

    def add_bulk(
        self,
        trading_results: List[models.TradingResult],
    ) -> List[models.TradingResult]:
        """Добавляет список данных о результатах торгов в репозиторий"""
        if any(res for res in trading_results if res.id is not None):
            raise ValueError('You are not allowed to specify record ID manually')
        
        for trading_result in trading_results:
            trading_result.id = uuid.uuid4()
            self._uncommitted_data.append(trading_result)
        
        return trading_results
    

    async def list(
        self,
        result_filter: Optional[filters.TradingResultFilter] = None,
        distinct_on: Optional[str] = None,
        order_by: Optional[str] = None,
        ascending: bool = True,
        limit: Optional[int] = None,
    ) -> List[models.TradingResult]:
        trading_results = list(self._data.values())

        if result_filter is not None:
            trading_results = self._filter(trading_results, result_filter)
        
        if distinct_on is not None:
            trading_results = self._distinct_on(trading_results, distinct_on)

        if order_by is not None:
            trading_results = self._order(
                trading_results,
                order_by=order_by,
                ascending=ascending,
            )
        
        if limit is not None:
            return self._limit(trading_results, limit)

        return trading_results
    

    def _filter(
        self,
        trading_results: List[models.TradingResult],
        result_filter: filters.TradingResultFilter,
    ) -> List[models.TradingResult]:
        filtered_results: List[models.TradingResult] = []

        for trading_result in trading_results:
            if self._matches_filter(trading_result, result_filter):
                filtered_results.append(trading_result)
        
        return filtered_results
    

    def _matches_filter(
        self,
        trading_result: models.TradingResult,
        result_filter: filters.TradingResultFilter,
    ) -> bool:
        if result_filter.oil_id is not None:
            if trading_result.oil_id != result_filter.oil_id:
                return False
        
        if result_filter.delivery_type_id is not None:
            if trading_result.delivery_type_id != result_filter.delivery_type_id:
                return False
        
        if result_filter.delivery_basis_id is not None:
            if trading_result.delivery_basis_id != result_filter.delivery_basis_id:
                return False
        
        if result_filter.start_date is not None:
            if trading_result.date < result_filter.start_date:
                return False
        
        if result_filter.end_date is not None:
            if trading_result.date > result_filter.end_date:
                return False
        
        return True
    

    def _distinct_on(
        self,
        trading_results: List[models.TradingResult],
        column_name: str,
    ) -> List[models.TradingResult]:
        grouped_results: List[models.TradingResult] = []
        attribute_values: Set[Any] = set()

        for trading_result in trading_results:
            attribute_value = getattr(trading_result, column_name)

            if attribute_value in attribute_values:
                continue
            
            attribute_values.add(attribute_value)
            grouped_results.append(trading_result)
        
        return grouped_results
    

    def _order(
        self,
        trading_results: List[models.TradingResult],
        order_by: str,
        ascending: bool,
    ) -> List[models.TradingResult]:
        sorting_key = lambda item: getattr(item, order_by)
        return sorted(trading_results, key=sorting_key, reverse=not ascending)
    

    def _limit(
        self,
        trading_results: List[models.TradingResult],
        limit: int,
    ) -> List[models.TradingResult]:
        return trading_results[:limit]
    

    def commit(self) -> None:
        self._data.update({res.id: res for res in self._uncommitted_data}) # type: ignore
    

    def rollback(self) -> None:
        self._uncommitted_data.clear()
