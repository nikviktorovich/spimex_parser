import datetime

import pytest
import sqlalchemy.engine

from spimex_parser.domain import models
from spimex_parser.modules.data_storage import unit_of_work


def create_trading_result() -> models.TradingResult:
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
        date=datetime.date(year=2023, month=9, day=21),
        created_on=datetime.datetime.now(),
        updated_on=datetime.datetime.now(),
    )


@pytest.mark.usefixtures('engine')
def test_add_trading_result(engine: sqlalchemy.engine.Engine) -> None:
    with unit_of_work.SqlAlchemyTradingResultsUnitOfWork(engine) as uow:
        trading_result = create_trading_result()
        uow.data.add(trading_result=trading_result)
        uow.commit()

        assert uow.data.list()


@pytest.mark.usefixtures('engine')
def test_add_trading_results(engine: sqlalchemy.engine.Engine) -> None:
    with unit_of_work.SqlAlchemyTradingResultsUnitOfWork(engine) as uow:
        trading_results = [
            create_trading_result(),
            create_trading_result(),
        ]
        uow.data.add_bulk(trading_results=trading_results)
        uow.commit()

        assert len(uow.data.list()) == 2


@pytest.mark.usefixtures('engine')
def test_get_trading_result(engine: sqlalchemy.engine.Engine) -> None:
    with unit_of_work.SqlAlchemyTradingResultsUnitOfWork(engine) as uow:
        trading_result = create_trading_result()
        added_trading_result = uow.data.add(trading_result=trading_result)
        uow.commit()

        assert added_trading_result.id is not None
        assert uow.data.get(trading_result_id=added_trading_result.id) is not None
