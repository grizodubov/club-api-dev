import re
import asyncio
import orjson

from app.core.context import get_api_context



EVENTS = {
    'user_login': { 'send': False, 'filter_params': { 'password', 'code' } },
    'user_logout': { 'send': False, 'filter_params': None },
    'user_update': { 'send': True, 'filter_params': None },
    'user_add_contact': { 'send': True, 'filter_params': None },
    'user_del_contact': { 'send': True, 'filter_params': None },
    'user_add_event': { 'send': True, 'filter_params': None },
    'user_del_event': { 'send': True, 'filter_params': None },
    'user_thumbs_up': { 'send': True, 'filter_params': None },
    'message_add': { 'send': True, 'filter_params': None },
    'message_view': { 'send': True, 'filter_params': None },
}



################################################################
def dispatch(event, request, user_id = None):
    req = {
        'event': event,
        'user_id': user_id if user_id is not None else request.user.id,
        'path': request.url.path,
        'params': dict(request.params),
    }
    asyncio.create_task(send_event_message(req))
    asyncio.create_task(register_event(req))



################################################################
async def send_event_message(req):
    api = get_api_context()
    message = {}
    if req['event'] in EVENTS and EVENTS[req['event']]['send']:
        message['event'] = req['event']
    if message:
        api.websocket_mass_send(message)



################################################################
async def register_event(req):
    api = get_api_context()
    if req['event'] in EVENTS and EVENTS[req['event']]['filter_params']:
        for param in EVENTS[req['event']]['filter_params']:
            if param in req['params']:
                req['params'][param] = '***'
    await api.pg.club.execute(
        """INSERT INTO
                log (event, user_id, path, params)
            VALUES
                ($1, $2, $3, $4)""",
        req['event'], req['user_id'], req['path'], req['params']
    )
