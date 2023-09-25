import uuid
from typing import List
from typing import Optional

import sqlalchemy.orm

from spimex_parser.modules.data_storage.database import models as db_models
from spimex_parser.domain import models


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
    

    def list(self) -> List[models.TradingResult]:
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
    

    def list(self) -> List[models.TradingResult]:
        db_trading_results = self.session.query(db_models.TradingResult).all()
        return [self._to_domain_model(res) for res in db_trading_results]
    

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
