import orjson
from starlette.responses import JSONResponse

from app.core.context import get_request_context



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
        # user
        user = {
            '_user': {
                'id': request.user.id,
                'name': request.user.name,
                'status': request.user.status,
                'avatar': request.user.avatar,
            }
        }
        return orjson.dumps(content | token | user)
