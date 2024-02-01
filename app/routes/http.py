from app.routes.endpoints.home import routes as routes_home
from app.routes.endpoints.item import routes as routes_item
from app.routes.endpoints.auth import routes as routes_auth
from app.routes.endpoints.user import routes as routes_user
from app.routes.endpoints.event import routes as routes_event
from app.routes.endpoints.news import routes as routes_news
from app.routes.endpoints.feed import routes as routes_feed
from app.routes.endpoints.message import routes as routes_message
from app.routes.endpoints.group import routes as routes_group
from app.routes.endpoints.community import routes as routes_community
from app.routes.endpoints.stats import routes as routes_stats
from app.routes.endpoints.avatar import routes as routes_avatar
from app.routes.endpoints.notification import routes as routes_notification
from app.routes.endpoints.tag import routes as routes_tag
from app.routes.endpoints.poll import routes as routes_poll
from app.routes.endpoints.log import routes as routes_log
from app.routes.endpoints.note import routes as routes_note
from app.routes.endpoints.agent_note import routes as routes_agent_note

from app.routes.endpoints.telegram import routes as routes_telegram


routes = [
    *routes_home(),
    *routes_item(),
    *routes_auth(),
    *routes_user(),
    *routes_event(),
    *routes_news(),
    *routes_feed(),
    *routes_message(),
    *routes_group(),
    *routes_community(),
    *routes_stats(),
    *routes_avatar(),
    *routes_notification(),
    *routes_tag(),
    *routes_poll(),
    *routes_log(),
    *routes_note(),
    *routes_agent_note(),

    *routes_telegram(),
]
