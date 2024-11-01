from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.gzip import GZipMiddleware
from starlette.middleware.cors import CORSMiddleware

from app.core.request import ReqMiddleware
from app.core.api import API
from app.core.request import extend_request
from app.routes.http import routes as http_routes
from app.routes.websocket import routes as websocket_routes



async def startup():
    app.state.api = API()
    await app.state.api.init()



app = Starlette(
    debug = False,
    middleware = [
        Middleware(
            GZipMiddleware,
            minimum_size = 512,
        ),
        Middleware(
            CORSMiddleware,
            allow_origins = [ '*' ],
            allow_methods = [ '*' ],
            allow_headers = [ '*' ],
            expose_headers = [
                'x-binding-notifications',
            ],
            allow_credentials = False,
        ),
        Middleware(
            ReqMiddleware,
        ),
    ],
    routes = [
        *http_routes,
        *websocket_routes,
    ],
    on_startup = [
        startup,
        extend_request,
    ],
)
