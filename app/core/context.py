from contextvars import ContextVar
from starlette.requests import Request

from app.core.api import API



_API: ContextVar[API] = ContextVar('API', default = None)

_REQUEST: ContextVar[Request] = ContextVar('REQUEST', default = None)



####################################################################
def get_api_context() -> API:
    return _API.get()



####################################################################
def set_api_context(api):
    return _API.set(api)



####################################################################
def get_request_context() -> Request:
    return _REQUEST.get()



####################################################################
def set_request_context(request):
    return _REQUEST.set(request)
