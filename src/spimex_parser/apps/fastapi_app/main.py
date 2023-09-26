import fastapi

import spimex_parser.apps.fastapi_app.dynamics.routers


app = fastapi.FastAPI()
app.include_router(spimex_parser.apps.fastapi_app.dynamics.routers.router)
