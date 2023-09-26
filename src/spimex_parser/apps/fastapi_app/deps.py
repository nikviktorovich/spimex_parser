from collections.abc import AsyncIterable

import sqlalchemy.ext.asyncio.engine
from fastapi import Depends

from spimex_parser.apps.fastapi_app import database
from spimex_parser.modules.data_storage.asyncio import unit_of_work


async def get_engine() -> sqlalchemy.ext.asyncio.engine.AsyncEngine:
    return database.engine


async def get_uow(
    engine: sqlalchemy.ext.asyncio.engine.AsyncEngine = Depends(get_engine),
) -> AsyncIterable[unit_of_work.AsyncTradingResultsUnitOfWork]:
    uow = unit_of_work.AsyncSqlAlchemyTradingResultsUnitOfWork(engine)
    async with uow:
        yield uow
