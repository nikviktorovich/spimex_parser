from typing import List
from typing import Optional

import fastapi
import fastapi_cache.decorator
from fastapi import Depends

from spimex_parser.apps.fastapi_app import deps
from spimex_parser.apps.fastapi_app.trading_results import serializers
from spimex_parser.modules.data_storage import filters
from spimex_parser.modules.data_storage.asyncio import unit_of_work


router = fastapi.APIRouter(
    prefix='/trading_results',
    tags=['trading_results'],
)


@router.get('/', response_model=List[serializers.TradingResultRead])
@fastapi_cache.decorator.cache()
async def list_trading_results(
    uow: unit_of_work.AsyncTradingResultsUnitOfWork = Depends(deps.get_uow),
    oil_id: Optional[str] = None,
    delivery_type_id: Optional[str] = None,
    delivery_basis_id: Optional[str] = None,
):
    result_filter = filters.TradingResultFilter(
        oil_id=oil_id,
        delivery_type_id=delivery_type_id,
        delivery_basis_id=delivery_basis_id,
    )
    filtered_trading_results = await uow.data.list(
        result_filter,
        order_by='date',
        ascending=False,
    )
    
    serialized_results: List[serializers.TradingResultRead] = []
    for trading_result in filtered_trading_results:
        serialized_result = serializers.TradingResultRead.model_validate(trading_result)
        serialized_results.append(serialized_result)
    
    return serialized_results
