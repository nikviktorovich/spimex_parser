from collections.abc import AsyncIterable
from collections.abc import Iterable

import aiohttp
import fastapi
import fastapi.testclient
import pytest
import pytest_asyncio
import sqlalchemy
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.engine
import sqlalchemy.orm

import spimex_parser.apps.fastapi_app.main
from spimex_parser import config
from spimex_parser.database import models as db_models


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


@pytest_asyncio.fixture
async def async_engine() -> sqlalchemy.ext.asyncio.engine.AsyncEngine:
    aengine = sqlalchemy.ext.asyncio.engine.create_async_engine(
        config.ASYNC_DB_CONNECTION_URL,
    )
    await drop_tables(aengine)
    await create_tables(aengine)
    return aengine


async def drop_tables(engine: sqlalchemy.ext.asyncio.engine.AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(db_models.Base.metadata.drop_all)


async def create_tables(engine: sqlalchemy.ext.asyncio.engine.AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(db_models.Base.metadata.create_all)


@pytest.fixture
def fastapi_app() -> Iterable[fastapi.FastAPI]:
    yield spimex_parser.apps.fastapi_app.main.app
    spimex_parser.apps.fastapi_app.main.app.dependency_overrides.clear()


@pytest.fixture
def fastapi_test(
    fastapi_app: fastapi.FastAPI,
) -> Iterable[fastapi.testclient.TestClient]:
    test_client = fastapi.testclient.TestClient(fastapi_app)
    with test_client:
        yield test_client
