import uuid
from typing import List
from typing import Optional

import sqlalchemy.orm
from sqlalchemy import asc
from sqlalchemy import desc

from spimex_parser.database import models as db_models
from spimex_parser.domain import models
from spimex_parser.modules.data_storage import filters


class TradingResultsRepository:
    """Репозиторий хранилища данных о результатах торгов со Spimex"""
    def get(self, trading_result_id: uuid.UUID) -> Optional[models.TradingResult]:
        """Возвращает данных о результате торгов с указанным id
        
        Возвращает данных о результате торгов с указанным id или None в случае,
        если записи с таким id не существует
        """
        raise NotImplementedError()
    

    def add(self, trading_result: models.TradingResult) -> models.TradingResult:
        """Добавляет данные о результатах сделки в репозиторий"""
        raise NotImplementedError()
    

    def add_bulk(
        self,
        trading_results: List[models.TradingResult],
    ) -> List[models.TradingResult]:
        """Добавляет список данных о результатах торгов в репозиторий"""
        raise NotImplementedError()
    

    def list(
        self,
        result_filter: Optional[filters.TradingResultFilter] = None,
        order_by: Optional[str] = None,
        ascending: bool = True,
    ) -> List[models.TradingResult]:
        raise NotImplementedError()


class SqlAlchemyTradingResultRepository(TradingResultsRepository):
    """Репозиторий SQL хранилища данных о результатах торгов со Spimex"""
    session: sqlalchemy.orm.Session


    def __init__(self, session: sqlalchemy.orm.Session) -> None:
        self.session = session


    def get(self, trading_result_id: uuid.UUID) -> Optional[models.TradingResult]:
        """Возвращает данных о результате торгов с указанным id
        
        Возвращает данных о результате торгов с указанным id или None в случае,
        если записи с таким id не существует
        """
        record = self.session.get(db_models.TradingResult, trading_result_id)

        if record is None:
            return None
        
        return self._to_domain_model(record)
    

    def add(self, trading_result: models.TradingResult) -> models.TradingResult:
        """Добавляет данные о результатах сделки в репозиторий"""
        if trading_result.id is not None:
            raise ValueError('You are not allowed to specify record ID manually')
        
        db_trading_result = self._from_domain_model(trading_result)
        db_trading_result.id = uuid.uuid4()
        self.session.add(db_trading_result)

        return self._to_domain_model(db_trading_result)
    

    def add_bulk(
        self,
        trading_results: List[models.TradingResult],
    ) -> List[models.TradingResult]:
        """Добавляет список данных о результатах торгов в репозиторий"""
        if any(res for res in trading_results if res.id is not None):
            raise ValueError('You are not allowed to specify record ID manually')
        
        db_trading_results: List[db_models.TradingResult] = []

        for trading_result in trading_results:
            db_trading_result = self._from_domain_model(trading_result)
            db_trading_result.id = uuid.uuid4()
            db_trading_results.append(db_trading_result)

        self.session.add_all(db_trading_results)

        added_results = [self._to_domain_model(res) for res in db_trading_results]

        return added_results
    

    def _from_domain_model(
        self,
        trading_result: models.TradingResult,
    ) -> db_models.TradingResult:
        """Преобразует доменную модель данных в формат модели базы данных"""
        return db_models.TradingResult(
            id=trading_result.id,
            exchange_product_id=trading_result.exchange_product_id,
            exchange_product_name=trading_result.exchange_product_name,
            oil_id=trading_result.oil_id,
            delivery_basis_id=trading_result.delivery_basis_id,
            delivery_basis_name=trading_result.delivery_basis_name,
            delivery_type_id=trading_result.delivery_type_id,
            volume=trading_result.volume,
            total=trading_result.total,
            count=trading_result.count,
            date=trading_result.date,
            created_on=trading_result.created_on,
            updated_on=trading_result.updated_on,
        )
    

    def list(
        self,
        result_filter: Optional[filters.TradingResultFilter] = None,
        group_by: Optional[str] = None,
        order_by: Optional[str] = None,
        ascending: bool = True,
        limit: Optional[int] = None,
    ) -> List[models.TradingResult]:
        query = self.session.query(db_models.TradingResult)

        if result_filter is not None:
            query = self._filter_query(query, result_filter=result_filter)
        
        if group_by is not None:
            query = self._group_query(query, group_by=group_by)

        if order_by is not None:
            query = self._order_query(
                query,
                order_by=order_by,
                ascending=ascending,
            )
        
        if limit is not None:
            query = self._limit_query(query, limit)

        db_trading_results = query.all()
        return [self._to_domain_model(res) for res in db_trading_results]


    def _filter_query(
        self,
        query: sqlalchemy.orm.Query[db_models.TradingResult],
        result_filter: filters.TradingResultFilter,
    ) -> sqlalchemy.orm.Query[db_models.TradingResult]:
        if result_filter.oil_id is not None:
            query = query.filter_by(oil_id=result_filter.oil_id)
        
        if result_filter.delivery_type_id is not None:
            query = query.filter_by(
                delivery_type_id=result_filter.delivery_type_id,
            )
        
        if result_filter.delivery_basis_id is not None:
            query = query.filter_by(
                delivery_basis_id=result_filter.delivery_basis_id,
            )
        
        if result_filter.start_date is not None:
            query = query.filter(
                db_models.TradingResult.date >= result_filter.start_date,
            )
        
        if result_filter.end_date is not None:
            query = query.filter(
                db_models.TradingResult.date <= result_filter.end_date,
            )
        
        return query


    def _group_query(
        self,
        query: sqlalchemy.orm.Query[db_models.TradingResult],
        group_by: str,
    ) -> sqlalchemy.orm.Query[db_models.TradingResult]:
        return query.group_by(group_by)


    def _order_query(
        self,
        query: sqlalchemy.orm.Query[db_models.TradingResult],
        order_by: str,
        ascending: bool,
    ) -> sqlalchemy.orm.Query[db_models.TradingResult]:
        order = asc(order_by) if ascending is True else desc(order_by)
        return query.order_by(order)
    

    def _limit_query(
        self,
        query: sqlalchemy.orm.Query[db_models.TradingResult],
        limit: int,
    ) -> sqlalchemy.orm.Query[db_models.TradingResult]:
        return query.limit(limit)


    def _to_domain_model(
        self,
        trading_result: db_models.TradingResult,
    ) -> models.TradingResult:
        return models.TradingResult(
            id=trading_result.id,
            exchange_product_id=trading_result.exchange_product_id,
            exchange_product_name=trading_result.exchange_product_name,
            oil_id=trading_result.oil_id,
            delivery_basis_id=trading_result.delivery_basis_id,
            delivery_basis_name=trading_result.delivery_basis_name,
            delivery_type_id=trading_result.delivery_type_id,
            volume=trading_result.volume,
            total=trading_result.total,
            count=trading_result.count,
            date=trading_result.date,
            created_on=trading_result.created_on,
            updated_on=trading_result.updated_on,
        )
