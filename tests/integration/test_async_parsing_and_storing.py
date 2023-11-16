import aiohttp
import pytest
import sqlalchemy.ext.asyncio.engine

from spimex_parser.modules.data_storage.asyncio import unit_of_work as data_unit_of_work
from spimex_parser.modules.parser.asyncio import unit_of_work as parser_unit_of_work


@pytest.mark.skip(reason='Нестабильная загрузка с сайта')
@pytest.mark.usefixtures('async_engine', 'async_client')
@pytest.mark.asyncio
async def test_async_parse_and_add_trading_results(
    async_engine: sqlalchemy.ext.asyncio.engine.AsyncEngine,
    async_client: aiohttp.ClientSession,
) -> None:
    url = 'https://spimex.com/upload/reports/oil_xls/oil_xls_20230921162000.xls'
    uow = parser_unit_of_work.AsyncPandasSpimexTradingResultsUnitOfWork(
        oil_data_path=url,
        client=async_client,
    )
    async with uow:
        trading_results = await uow.data.list()

    uow = data_unit_of_work.AsyncSqlAlchemyTradingResultsUnitOfWork(async_engine)
    async with uow:
        assert not await uow.data.list()

        uow.data.add_bulk(trading_results=trading_results)
        await uow.commit()

        assert await uow.data.list()
