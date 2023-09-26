import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine

from spimex_parser import config


engine = sqlalchemy.ext.asyncio.create_async_engine(config.ASYNC_DB_CONNECTION_URL)
