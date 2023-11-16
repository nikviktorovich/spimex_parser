import aiohttp
import aiohttp_retry
import contextlib
from collections.abc import AsyncIterator

from spimex_parser.apps.console_async import database
from spimex_parser.modules.data_storage.asyncio import unit_of_work as data_unit_of_work
from spimex_parser.modules.parser.asyncio import unit_of_work as parser_unit_of_work


@contextlib.asynccontextmanager
async def get_data_uow() -> AsyncIterator[data_unit_of_work.AsyncTradingResultsUnitOfWork]:
    engine = database.engine
    uow = data_unit_of_work.AsyncSqlAlchemyTradingResultsUnitOfWork(engine)
    async with uow:
        yield uow


@contextlib.asynccontextmanager
async def get_parser_uow(
    url: str,
    client: aiohttp.ClientSession,
) -> AsyncIterator[parser_unit_of_work.AsyncSpimexTradingResultsUnitOfWork]:
    uow = parser_unit_of_work.AsyncPandasSpimexTradingResultsUnitOfWork(
        oil_data_path=url,
        client=client,
    )
    async with uow:
        yield uow


@contextlib.asynccontextmanager
async def get_async_client() -> AsyncIterator[aiohttp.ClientSession]:
    timeout = aiohttp.ClientTimeout(connect=15, total=30)
    connector = aiohttp.TCPConnector(limit=100)
    client = aiohttp.ClientSession(timeout=timeout, connector=connector)

    async with client:
        retry = aiohttp_retry.JitterRetry(attempts=10)
        retry_client = aiohttp_retry.RetryClient(
            client_session=client,
            retry_options=retry,
            raise_for_status=True,
        )
        yield retry_client # type: ignore
