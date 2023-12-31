import datetime
from typing import List
from typing import Optional

import fastapi
import fastapi_cache.decorator
from fastapi import Depends

from spimex_parser.apps.fastapi_app import deps
from spimex_parser.apps.fastapi_app.last_trading_dates import schemas
from spimex_parser.modules.data_storage import filters
from spimex_parser.modules.data_storage.asyncio import unit_of_work


router = fastapi.APIRouter(
    prefix='/last_trading_dates',
    tags=['last_trading_dates'],
)


@router.get('/', response_model=List[schemas.TradingDateRead])
@fastapi_cache.decorator.cache()
async def list_last_trading_dates(
    uow: unit_of_work.AsyncTradingResultsUnitOfWork = Depends(deps.get_uow),
    limit: Optional[int] = None,
):
    last_dates_trading_results = await uow.data.list(distinct_on='date', limit=limit)

    serialized_results: List[schemas.TradingDateRead] = []
    for trading_result in last_dates_trading_results:
        serialized_result = schemas.TradingDateRead(date=trading_result.date)
        serialized_results.append(serialized_result)
    
    return serialized_results
