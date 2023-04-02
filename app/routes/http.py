from app.routes.endpoints.home import routes as routes_home
from app.routes.endpoints.auth import routes as routes_auth



routes = [
    *routes_home(),
    *routes_auth(),
]
