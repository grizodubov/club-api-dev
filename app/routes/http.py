from app.routes.endpoints.home import routes as routes_home
from app.routes.endpoints.auth import routes as routes_auth
from app.routes.endpoints.user import routes as routes_user
from app.routes.endpoints.event import routes as routes_event
from app.routes.endpoints.message import routes as routes_message


routes = [
    *routes_home(),
    *routes_auth(),
    *routes_user(),
    *routes_event(),
    *routes_message(),
]
