from typing import Callable

import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine

from spimex_parser.modules.data_storage.asyncio import repositories


class AsyncTradingResultsUnitOfWork:
    """Асинхронная единица работы с хранилищем данных о результатах торгов
    со Spimex
    """
    data: repositories.AsyncTradingResultsRepository


    async def __aenter__(self) -> 'AsyncTradingResultsUnitOfWork':
        raise NotImplementedError()
    

    async def __aexit__(self, *args, **kwargs) -> None:
        raise NotImplementedError()
    

    async def commit(self) -> None:
        raise NotImplementedError()
    

    async def rollback(self) -> None:
        raise NotImplementedError()


class AsyncSqlAlchemyTradingResultsUnitOfWork(AsyncTradingResultsUnitOfWork):
    """Асинхронная диница работы с хранилищем данных о результатах торгов
    со Spimex
    """
    session_factory: Callable[[], sqlalchemy.ext.asyncio.AsyncSession]
    session: sqlalchemy.ext.asyncio.AsyncSession


    def __init__(self, engine: sqlalchemy.ext.asyncio.engine.AsyncEngine) -> None:
        self.session_factory = sqlalchemy.ext.asyncio.async_sessionmaker(
            bind=engine,
            expire_on_commit=False,
        )
    

    async def __aenter__(self) -> AsyncTradingResultsUnitOfWork:
        self.session = self.session_factory()
        self.data = repositories.AsyncSqlAlchemyTradingResultRepository(self.session)
        return self
    

    async def __aexit__(self, *args, **kwargs) -> None:
        await self.session.close()
    

    async def commit(self) -> None:
        await self.session.commit()
    

    async def rollback(self) -> None:
        await self.session.rollback()
