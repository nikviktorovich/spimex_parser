import datetime

import pytest
import sqlalchemy.engine

from spimex_parser.domain import models
from spimex_parser.modules.data_storage import filters
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
def test_filter_by_oil_id(engine: sqlalchemy.engine.Engine) -> None:
    with unit_of_work.SqlAlchemyTradingResultsUnitOfWork(engine) as uow:
        trading_result = create_trading_result()
        uow.data.add(trading_result=trading_result)
        uow.commit()

        assert uow.data.filter(filters.TradingResultFilter(oil_id='A100'))
        assert not uow.data.filter(filters.TradingResultFilter(oil_id='A200'))


@pytest.mark.usefixtures('engine')
def test_filter_by_delivery_type_id(engine: sqlalchemy.engine.Engine) -> None:
    with unit_of_work.SqlAlchemyTradingResultsUnitOfWork(engine) as uow:
        trading_result = create_trading_result()
        uow.data.add(trading_result=trading_result)
        uow.commit()

        assert uow.data.filter(filters.TradingResultFilter(delivery_type_id='F'))
        assert not uow.data.filter(filters.TradingResultFilter(delivery_type_id='T'))


@pytest.mark.usefixtures('engine')
def test_filter_by_delivery_basis_id(engine: sqlalchemy.engine.Engine) -> None:
    with unit_of_work.SqlAlchemyTradingResultsUnitOfWork(engine) as uow:
        trading_result = create_trading_result()
        uow.data.add(trading_result=trading_result)
        uow.commit()

        assert uow.data.filter(filters.TradingResultFilter(delivery_basis_id='NVY'))
        assert not uow.data.filter(filters.TradingResultFilter(delivery_basis_id='NBA'))


@pytest.mark.usefixtures('engine')
def test_filter_by_start_date(engine: sqlalchemy.engine.Engine) -> None:
    with unit_of_work.SqlAlchemyTradingResultsUnitOfWork(engine) as uow:
        trading_result = create_trading_result()
        uow.data.add(trading_result=trading_result)
        uow.commit()

        start_date = datetime.date(year=2023, month=9, day=1)
        assert uow.data.filter(filters.TradingResultFilter(start_date=start_date))

        start_date = datetime.date(year=2023, month=9, day=30)
        assert not uow.data.filter(filters.TradingResultFilter(start_date=start_date))


@pytest.mark.usefixtures('engine')
def test_filter_by_end_date(engine: sqlalchemy.engine.Engine) -> None:
    with unit_of_work.SqlAlchemyTradingResultsUnitOfWork(engine) as uow:
        trading_result = create_trading_result()
        uow.data.add(trading_result=trading_result)
        uow.commit()

        end_date = datetime.date(year=2023, month=9, day=30)
        assert uow.data.filter(filters.TradingResultFilter(end_date=end_date))

        end_date = datetime.date(year=2023, month=9, day=1)
        assert not uow.data.filter(filters.TradingResultFilter(end_date=end_date))


@pytest.mark.usefixtures('engine')
def test_filter_by_date_range(engine: sqlalchemy.engine.Engine) -> None:
    with unit_of_work.SqlAlchemyTradingResultsUnitOfWork(engine) as uow:
        trading_result = create_trading_result()
        uow.data.add(trading_result=trading_result)
        uow.commit()

        start_date = datetime.date(year=2023, month=9, day=1)
        end_date = datetime.date(year=2023, month=9, day=30)
        result_filter = filters.TradingResultFilter(
            start_date=start_date,
            end_date=end_date,
        )
        assert uow.data.filter(result_filter)

        start_date = datetime.date(year=2023, month=9, day=23)
        end_date = datetime.date(year=2023, month=9, day=30)
        result_filter = filters.TradingResultFilter(
            start_date=start_date,
            end_date=end_date,
        )
        assert not uow.data.filter(result_filter)

        start_date = datetime.date(year=2023, month=9, day=1)
        end_date = datetime.date(year=2023, month=9, day=20)
        result_filter = filters.TradingResultFilter(
            start_date=start_date,
            end_date=end_date,
        )
        assert not uow.data.filter(result_filter)
