import datetime
import uuid
from typing import List

import fastapi
import fastapi.testclient
import pytest
from fastapi import status

from spimex_parser.apps.fastapi_app import deps
from spimex_parser.domain import models

from tests.fakes.data_storage import repositories
from tests.fakes.data_storage import unit_of_work


def create_trading_result_with_date(date: datetime.date) -> models.TradingResult:
    return models.TradingResult(
        id=uuid.uuid4(),
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


@pytest.mark.usefixtures('fastapi_app', 'fastapi_test')
def test_last_trading_dates_endpoint_empty_list(
    fastapi_app: fastapi.FastAPI,
    fastapi_test: fastapi.testclient.TestClient,
) -> None:
    repo = repositories.AsyncFakeTradingResultsRepository([])
    uow = unit_of_work.FakeTradingResultsUnitOfWork(repo)
    fastapi_app.dependency_overrides[deps.get_uow] = lambda: uow
    
    response = fastapi_test.get('/last_trading_dates')
    assert response.status_code == status.HTTP_200_OK
    assert not response.json()


@pytest.mark.usefixtures('fastapi_app', 'fastapi_test')
def test_last_trading_dates_endpoint_limit(
    fastapi_app: fastapi.FastAPI,
    fastapi_test: fastapi.testclient.TestClient,
) -> None:
    trading_results = [
        create_trading_result_with_date(datetime.date(year=2023, month=9, day=30)),
        create_trading_result_with_date(datetime.date(year=2023, month=9, day=20)),
    ]
    repo = repositories.AsyncFakeTradingResultsRepository(trading_results)
    uow = unit_of_work.FakeTradingResultsUnitOfWork(repo)
    fastapi_app.dependency_overrides[deps.get_uow] = lambda: uow
    
    response = fastapi_test.get('/last_trading_dates')
    assert response.status_code == status.HTTP_200_OK
    assert len(response.json()) == 2
    
    response = fastapi_test.get('/last_trading_dates?limit=1')
    assert response.status_code == status.HTTP_200_OK
    assert len(response.json()) == 1

    response = fastapi_test.get('/last_trading_dates?limit=0')
    assert response.status_code == status.HTTP_200_OK
    assert not response.json()
