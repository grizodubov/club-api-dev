from app.routes.endpoints.home import routes as routes_home
from app.routes.endpoints.auth import routes as routes_auth
from app.routes.endpoints.user import routes as routes_user
from app.routes.endpoints.event import routes as routes_event
from app.routes.endpoints.news import routes as routes_news
from app.routes.endpoints.feed import routes as routes_feed
from app.routes.endpoints.message import routes as routes_message
from app.routes.endpoints.group import routes as routes_group
from app.routes.endpoints.community import routes as routes_community
from app.routes.endpoints.stats import routes as routes_stats


routes = [
    *routes_home(),
    *routes_auth(),
    *routes_user(),
    *routes_event(),
    *routes_news(),
    *routes_feed(),
    *routes_message(),
    *routes_group(),
    *routes_community(),
    *routes_stats(),
]
