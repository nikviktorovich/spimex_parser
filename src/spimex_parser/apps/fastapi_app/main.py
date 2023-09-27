import fastapi

import spimex_parser.apps.fastapi_app.dynamics.routers
import spimex_parser.apps.fastapi_app.trading_results.routers


app = fastapi.FastAPI()
app.include_router(spimex_parser.apps.fastapi_app.dynamics.routers.router)
app.include_router(spimex_parser.apps.fastapi_app.trading_results.routers.router)
