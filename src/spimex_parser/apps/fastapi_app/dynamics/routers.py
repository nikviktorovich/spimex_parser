import datetime
from typing import List
from typing import Optional

import fastapi
from fastapi import Depends

from spimex_parser.apps.fastapi_app import deps
from spimex_parser.apps.fastapi_app.dynamics import serializers
from spimex_parser.modules.data_storage import filters
from spimex_parser.modules.data_storage.asyncio import unit_of_work


router = fastapi.APIRouter(
    prefix='/dynamics',
    tags=['dynamics'],
)


@router.get('/', response_model=List[serializers.DynamicsRead])
async def list_dynamics(
    uow: unit_of_work.AsyncTradingResultsUnitOfWork = Depends(deps.get_uow),
    oil_id: Optional[str] = None,
    delivery_type_id: Optional[str] = None,
    delivery_basis_id: Optional[str] = None,
    start_date: Optional[datetime.date] = None,
    end_date: Optional[datetime.date] = None,
):
    result_filter = filters.TradingResultFilter(
        oil_id=oil_id,
        delivery_type_id=delivery_type_id,
        delivery_basis_id=delivery_basis_id,
        start_date=start_date,
        end_date=end_date,
    )
    filtered_trading_results = await uow.data.list(result_filter)
    
    serialized_results: List[serializers.DynamicsRead] = []
    for trading_result in filtered_trading_results:
        serialized_result = serializers.DynamicsRead.model_validate(trading_result)
        serialized_results.append(serialized_result)
    
    return serialized_results
