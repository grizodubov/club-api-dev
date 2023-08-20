import asyncio
import orjson
from starlette.responses import JSONResponse

from app.core.context import get_request_context
from app.models.user import User



####################################################################
class OrjsonResponse(JSONResponse):


    ################################################################
    def render(self, content) -> bytes:
        request = get_request_context()
        # token
        if request.session.token_next:
            token = {
                '_token': request.session.token_next,
                '_time': request.session.time_last_activity
            }
        else:
            token = {}
        community_manager = {
            '_community_manager': {
                'id': request.community_manager.id,
                'name': request.community_manager.name,
                'phone': request.community_manager.phone,
            }
        }
        # user
        user = {
            '_user': {
                'id': request.user.id,
                'name': request.user.name,
                'status': request.user.status,
                'avatar': request.user.avatar,
                'roles': request.user.roles,
            }
        }
        return orjson.dumps(content | token | user | community_manager)
