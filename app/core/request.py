import orjson
from starlette.requests import Request
from starlette.middleware.base import BaseHTTPMiddleware

from app.core.response import OrjsonResponse
from app.core.context import set_api_context, set_request_context
from app.models.session import Session
from app.models.user import User



####################################################################
def extend_request():
    Request.api = property(lambda self: self.app.state.api)
    Request.params = property(lambda self: self.state.params)
    Request.filters = property(lambda self: self.state.filters)
    Request.session = property(lambda self: self.state.session)
    Request.user = property(lambda self: self.state.user)



####################################################################
class ReqMiddleware(BaseHTTPMiddleware):


    ################################################################
    async def dispatch(self, request, call_next):
        set_api_context(request.api)
        set_request_context(request)
        await before_request(request)
        # response = await call_next(request)
        try:
            response = await call_next(request)
        except Exception as e:
            print('-- URL:', request.url)
            print('-- PARAMS:', request.params)
            print('-- ERROR:', e)
            return err(500, str(e))
        await after_request(request, response)
        return response



####################################################################
async def before_request(request):
    print('---- Before request: begin')
    # request.params
    body = await request.body()
    params = None
    if body:
        try:
            params = orjson.loads(body)
        except JSONDecodeError as e:
            request.state.params = {}
            print('Request error:', e)
        else:
            request.state.params = { key: params[key] for key in params if not key.startswith('_') }
    else:
        request.state.params = {}
    # request.session
    request.state.session = Session()
    if request.url.path != '/acquire':
        if params:
            if '_key' in params:
                await request.state.session.auth_by_key(key = params['_key'])
            else:
                await request.state.session.auth_by_token(token = params['_token'] if '_token' in params else '')
    # request.user
    request.state.user = User()
    if request.url.path != '/acquire':
        await request.state.user.set(id = request.state.session.user_id)
    # request.filters
    request.state.filters = {}
    print('---- Before request: end')



####################################################################
async def after_request(request, response):
    print('---- After request: begin')
    if request.url.path != '/acquire':
        if request.state.session.token_next:
            response.headers['x-binding-messages'] = '0'
    print('---- After request: end')



################################################################
def err(status = 500, detail = ''):
    return OrjsonResponse(
        status_code = status,
        content = { '_alert': detail }
    )
