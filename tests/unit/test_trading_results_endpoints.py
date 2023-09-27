import datetime
import uuid

import fastapi
import fastapi.testclient
import pytest
from fastapi import status

from spimex_parser.apps.fastapi_app import deps
from spimex_parser.domain import models

from tests.fakes.data_storage import repositories
from tests.fakes.data_storage import unit_of_work


def create_trading_result() -> models.TradingResult:
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
        date=datetime.date(year=2023, month=9, day=21),
        created_on=datetime.datetime.now(),
        updated_on=datetime.datetime.now(),
    )


@pytest.mark.usefixtures('fastapi_app', 'fastapi_test')
def test_list_trading_results_endpoint_empty_list(
    fastapi_app: fastapi.FastAPI,
    fastapi_test: fastapi.testclient.TestClient,
) -> None:
    repo = repositories.AsyncFakeTradingResultsRepository([])
    uow = unit_of_work.FakeTradingResultsUnitOfWork(repo)
    fastapi_app.dependency_overrides[deps.get_uow] = lambda: uow
    
    response = fastapi_test.get('/trading_results')
    assert response.status_code == status.HTTP_200_OK
    assert not response.json()


@pytest.mark.usefixtures('fastapi_app', 'fastapi_test')
def test_list_trading_results_endpoint_non_empty_list(
    fastapi_app: fastapi.FastAPI,
    fastapi_test: fastapi.testclient.TestClient,
) -> None:
    repo = repositories.AsyncFakeTradingResultsRepository([
        create_trading_result(),
    ])
    uow = unit_of_work.FakeTradingResultsUnitOfWork(repo)
    fastapi_app.dependency_overrides[deps.get_uow] = lambda: uow
    
    response = fastapi_test.get('/trading_results')
    assert response.status_code == status.HTTP_200_OK
    assert response.json()


@pytest.mark.usefixtures('fastapi_app', 'fastapi_test')
def test_list_trading_results_endpoint_oil_id_filter(
    fastapi_app: fastapi.FastAPI,
    fastapi_test: fastapi.testclient.TestClient,
) -> None:
    trading_result = create_trading_result()
    repo = repositories.AsyncFakeTradingResultsRepository([trading_result])
    uow = unit_of_work.FakeTradingResultsUnitOfWork(repo)
    fastapi_app.dependency_overrides[deps.get_uow] = lambda: uow
    
    response = fastapi_test.get('/trading_results', params={
        'oil_id': 'A100',
    })
    assert response.status_code == status.HTTP_200_OK
    assert response.json()

    response = fastapi_test.get('/trading_results', params={
        'oil_id': 'A200',
    })
    assert response.status_code == status.HTTP_200_OK
    assert not response.json()


@pytest.mark.usefixtures('fastapi_app', 'fastapi_test')
def test_list_trading_results_endpoint_delivery_type_id_filter(
    fastapi_app: fastapi.FastAPI,
    fastapi_test: fastapi.testclient.TestClient,
) -> None:
    trading_result = create_trading_result()
    repo = repositories.AsyncFakeTradingResultsRepository([trading_result])
    uow = unit_of_work.FakeTradingResultsUnitOfWork(repo)
    fastapi_app.dependency_overrides[deps.get_uow] = lambda: uow
    
    response = fastapi_test.get('/trading_results', params={
        'delivery_type_id': 'F',
    })
    assert response.status_code == status.HTTP_200_OK
    assert response.json()

    response = fastapi_test.get('/trading_results', params={
        'delivery_type_id': 'A',
    })
    assert response.status_code == status.HTTP_200_OK
    assert not response.json()


@pytest.mark.usefixtures('fastapi_app', 'fastapi_test')
def test_list_trading_results_endpoint_delivery_basis_id_filter(
    fastapi_app: fastapi.FastAPI,
    fastapi_test: fastapi.testclient.TestClient,
) -> None:
    trading_result = create_trading_result()
    repo = repositories.AsyncFakeTradingResultsRepository([trading_result])
    uow = unit_of_work.FakeTradingResultsUnitOfWork(repo)
    fastapi_app.dependency_overrides[deps.get_uow] = lambda: uow
    
    response = fastapi_test.get('/trading_results', params={
        'delivery_basis_id': 'NVY',
    })
    assert response.status_code == status.HTTP_200_OK
    assert response.json()

    response = fastapi_test.get('/trading_results', params={
        'delivery_basis_id': 'NCY',
    })
    assert response.status_code == status.HTTP_200_OK
    assert not response.json()
