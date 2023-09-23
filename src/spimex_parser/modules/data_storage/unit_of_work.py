from typing import Callable

import sqlalchemy.engine
import sqlalchemy.orm

from spimex_parser.modules.data_storage import repositories


class TradingResultsUnitOfWork:
    data: repositories.TradingResultsRepository


    def __enter__(self) -> 'TradingResultsUnitOfWork':
        raise NotImplementedError()
    

    def __exit__(self, *args, **kwargs) -> None:
        raise NotImplementedError()
    

    def commit(self) -> None:
        raise NotImplementedError()
    

    def rollback(self) -> None:
        raise NotImplementedError()


class SqlAlchemyTradingResultsUnitOfWork(TradingResultsUnitOfWork):
    session_factory: Callable[[], sqlalchemy.orm.Session]
    session: sqlalchemy.orm.Session


    def __init__(self, engine: sqlalchemy.engine.Engine) -> None:
        self.session_factory = sqlalchemy.orm.sessionmaker(bind=engine)
    

    def __enter__(self) -> TradingResultsUnitOfWork:
        self.session = self.session_factory()
        self.data = repositories.SqlAlchemyTradingResultRepository(self.session)
        return self
    

    def __exit__(self, *args, **kwargs) -> None:
        self.session.close()
    

    def commit(self) -> None:
        self.session.commit()
    

    def rollback(self) -> None:
        self.session.rollback()
