import sqlalchemy.ext.asyncio

from spimex_parser import config
from spimex_parser.database import models as db_models


engine = sqlalchemy.ext.asyncio.create_async_engine(config.ASYNC_DB_CONNECTION_URL)


async def create_tables() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(db_models.Base.metadata.create_all)
