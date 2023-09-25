import aiohttp
import pytest
from typing import List

from spimex_parser.domain import models
from spimex_parser.modules.parser import unit_of_work
from spimex_parser.modules.parser.asyncio import unit_of_work as async_unit_of_work


sample_urls = [
    'https://spimex.com/upload/reports/oil_xls/oil_xls_20230921162000.xls',
    'https://spimex.com/upload/reports/oil_xls/oil_xls_20230913162000.xls',
]


def test_loading_from_url() -> None:
    for url in sample_urls:
        trading_results = get_trading_results_sync(url)
        assert trading_results


def get_trading_results_sync(url: str) -> List[models.TradingResult]:
    with unit_of_work.PandasSpimexTradingResultsUnitOfWork(url) as uow:
        trading_results = uow.data.list()
        return trading_results


@pytest.mark.usefixtures('async_client')
@pytest.mark.asyncio
async def test_async_loading_from_url(async_client: aiohttp.ClientSession) -> None:
    for url in sample_urls:
        trading_results = await get_trading_results_async(url, async_client)
        assert trading_results


async def get_trading_results_async(
    url: str,
    client: aiohttp.ClientSession,
) -> List[models.TradingResult]:
    async with async_unit_of_work.AsyncPandasSpimexTradingResultsUnitOfWork(url, client) as uow:
        trading_results = await uow.data.list()
        return trading_results


@pytest.mark.usefixtures('async_client')
@pytest.mark.asyncio
async def test_sync_and_async_loading_and_compare(
    async_client: aiohttp.ClientSession,
) -> None:
    for url in sample_urls:
        trading_results_sync = get_trading_results_sync(url)
        trading_results_async = await get_trading_results_async(url, async_client)
        assert are_trading_result_lists_equal(
            trading_results_sync,
            trading_results_async,
        )


def are_trading_result_lists_equal(
    left: List[models.TradingResult],
    right: List[models.TradingResult],
) -> bool:
    left_product_ids = [result.exchange_product_id for result in left]
    right_product_ids = [result.exchange_product_id for result in right]
    return all(product_id in right_product_ids for product_id in left_product_ids)
