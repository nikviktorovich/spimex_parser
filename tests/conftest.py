from collections.abc import AsyncIterable

import aiohttp
import pytest
import pytest_asyncio
import sqlalchemy
import sqlalchemy.engine
import sqlalchemy.orm

from spimex_parser import config
from spimex_parser.modules.data_storage.database import models as db_models


@pytest.fixture
def engine() -> sqlalchemy.engine.Engine:
    engine = sqlalchemy.create_engine(config.DB_CONNECTION_URL)
    db_models.Base.metadata.drop_all(bind=engine)
    db_models.Base.metadata.create_all(bind=engine)
    return engine


@pytest_asyncio.fixture
async def async_client() -> AsyncIterable[aiohttp.ClientSession]:
    async with aiohttp.ClientSession() as client:
        yield client
