import datetime
from typing import List

import pytest
import sqlalchemy.ext.asyncio.engine

from spimex_parser.domain import models
from spimex_parser.modules.data_storage.asyncio import unit_of_work


def create_trading_result_with_date(date: datetime.date) -> models.TradingResult:
    return models.TradingResult(
        exchange_product_id='A100NVY060F',
        exchange_product_name='Бензин (АИ-100-К5), ст. Новоярославская (ст. отправления)',
        oil_id='A100',
        delivery_basis_id='NVY',
        delivery_basis_name='ст. Новоярославская',
        delivery_type_id='F',
        volume=60,
        total=4_200_000,
        count=1,
        date=date,
        created_on=datetime.datetime.now(),
        updated_on=datetime.datetime.now(),
    )


@pytest.mark.usefixtures('async_engine')
@pytest.mark.asyncio
async def test_order_by_date(
    async_engine: sqlalchemy.ext.asyncio.engine.AsyncEngine,
) -> None:
    async with unit_of_work.AsyncSqlAlchemyTradingResultsUnitOfWork(async_engine) as uow:
        trading_results_asc = [
            create_trading_result_with_date(datetime.date(year=2023, month=9, day=21)),
            create_trading_result_with_date(datetime.date(year=2023, month=9, day=25)),
        ]
        uow.data.add_bulk(trading_results_asc)
        await uow.commit()

        asc_dates = get_dates(trading_results_asc)

        fetched_results = await uow.data.list(order_by='date', ascending=True)
        fetched_dates_asc = get_dates(fetched_results)
        assert asc_dates == fetched_dates_asc

        fetched_results = await uow.data.list(order_by='date', ascending=False)
        fetched_dates_desc = get_dates(fetched_results)
        assert list(reversed(asc_dates)) == fetched_dates_desc


def get_dates(
    trading_results: List[models.TradingResult],
) -> List[datetime.date]:
    return [trading_result.date for trading_result in trading_results]
