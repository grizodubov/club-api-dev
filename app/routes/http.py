from app.routes.endpoints.home import routes as routes_home
from app.routes.endpoints.auth import routes as routes_auth
from app.routes.endpoints.user import routes as routes_user



routes = [
    *routes_home(),
    *routes_auth(),
    *routes_user(),
]
