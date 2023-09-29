import datetime
from typing import List

import fastapi
import fastapi_cache.decorator
from fastapi import Depends
from fastapi import Query

from spimex_parser.apps.fastapi_app import deps
from spimex_parser.apps.fastapi_app.dynamics import schemas
from spimex_parser.modules.data_storage import filters
from spimex_parser.modules.data_storage.asyncio import unit_of_work


router = fastapi.APIRouter(
    prefix='/dynamics',
    tags=['dynamics'],
)


@router.get('/', response_model=List[schemas.DynamicsRead])
@fastapi_cache.decorator.cache()
async def list_dynamics(
    uow: unit_of_work.AsyncTradingResultsUnitOfWork = Depends(deps.get_uow),
    dynamics_filter: schemas.DynamicsFilter = Depends(),
):
    result_filter = filters.TradingResultFilter(
        oil_id=dynamics_filter.oil_id,
        delivery_type_id=dynamics_filter.delivery_type_id,
        delivery_basis_id=dynamics_filter.delivery_basis_id,
        start_date=dynamics_filter.start_date,
        end_date=dynamics_filter.end_date,
    )
    filtered_trading_results = await uow.data.list(result_filter)
    
    serialized_results: List[schemas.DynamicsRead] = []
    for trading_result in filtered_trading_results:
        serialized_result = schemas.DynamicsRead.model_validate(trading_result)
        serialized_results.append(serialized_result)
    
    return serialized_results
