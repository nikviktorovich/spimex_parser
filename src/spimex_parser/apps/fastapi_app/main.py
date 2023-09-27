import contextlib
import schedule
from collections.abc import AsyncIterator

import fastapi
import fastapi_cache
import fastapi_cache.backends.redis
import redis.asyncio

import spimex_parser.apps.fastapi_app.dynamics.routers
import spimex_parser.apps.fastapi_app.trading_results.routers
import spimex_parser.apps.fastapi_app.last_trading_dates.routers
from spimex_parser import config


def request_key_builder(
    func,
    namespace: str = "",
    *,
    request: fastapi.Request,
    response: fastapi.Response,
    **kwargs,
):
    request_hash = ":".join([
        namespace,
        request.method.lower(),
        request.url.path,
        repr(sorted(request.query_params.items()))
    ])
    return request_hash


@contextlib.asynccontextmanager
async def lifespan(app: fastapi.FastAPI) -> AsyncIterator[None]:
    redis_instance = redis.asyncio.from_url(config.REDIS_URL)
    backend = fastapi_cache.backends.redis.RedisBackend(redis_instance)
    fastapi_cache.FastAPICache.init(
        backend=backend,
        prefix='fastapi-cache',
        key_builder=request_key_builder,
    )
    yield


app = fastapi.FastAPI(lifespan=lifespan)
app.include_router(spimex_parser.apps.fastapi_app.dynamics.routers.router)
app.include_router(spimex_parser.apps.fastapi_app.trading_results.routers.router)
app.include_router(spimex_parser.apps.fastapi_app.last_trading_dates.routers.router)
