import asyncio
import orjson
from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.helpers.qr import create_code
from app.models.event import Event
from app.models.user import User
from app.models.notification import create_notifications
from app.models.notification_1 import create_multiple as create_notification_1_multi, create as create_notification_1



def routes():
    return [
        Route('/qr/show', qr_show, methods = [ 'POST' ]),
        Route('/qr/event/register', qr_event_register, methods = [ 'POST' ]),

        Route('/ma/qr/create', manager_qr_create, methods = [ 'POST' ]),
    ]



MODELS = {
    'qr_event_register': {
        'event_id': {
            'required': True,
            'type': 'int',
            'value_min': 1,
        },
    },
    'manager_qr_create': {
        'type': {
            'required': True,
            'type': 'str',
            'values': [ 'user', 'event' ],
        },
        'method': {
            'required': True,
            'type': 'str',
            'values': [ 'profile', 'registration' ],
        },
        'item_id': {
            'required': True,
            'type': 'int',
            'value_min': 1,
        },
    },
}


################################################################
async def qr_show(request):
    if request.user.id:
        file ='profile-' + str(request.user.id)
        data = {
            'type': 'user',
            'method': 'profile',
            'id': request.user.id
        }
        url = create_code('user', file, data)
        return OrjsonResponse({
            'url': url,
        })
    else:
        return err(403, 'Нет доступа')
        


################################################################
async def qr_event_register(request):
    if request.user.id:
        if validate(request.params, MODELS['qr_event_register']):
            event = Event()
            await event.set(id = request.params['event_id'])
            if event.id:
                if await request.user.check_event(event_id = event.id):
                    await request.user.audit_event(event_id = event.id, audit = 2)
                    create_notifications('user_arrive', request.user.id, request.user.id, request.params)
                    connections_ids = await request.user.get_event_connections_ids(event_id = event.id)
                    if connections_ids:
                        await create_notification_1_multi(
                            users_ids = connections_ids,
                            event = 'arrive', 
                            data = {
                                'user': {
                                    'id': request.user.id,
                                    'name': request.user.name,
                                    'hash': request.user.avatar_hash,
                                },
                                'event': {
                                    'id': event.id,
                                    'time_event': event.time_event,
                                    'format': event.format,
                                    'name': event.name,
                                }
                            }
                        )
                    if request.user.community_manager_id:
                        manager = User()
                        await manager.set(id = request.user.community_manager_id)
                        if manager.id:
                            await create_notification_1(
                                user_id = manager.id,
                                event = 'arrive', 
                                data = {
                                    'user': {
                                        'id': request.user.id,
                                        'name': request.user.name,
                                        'hash': request.user.avatar_hash,
                                    },
                                    'event': {
                                        'id': event.id,
                                        'time_event': event.time_event,
                                        'format': event.format,
                                        'name': event.name,
                                    }
                                },
                                mode = 'manager',
                            )
                    dispatch('user_update', request)
                    return OrjsonResponse({
                        '_popup': 'registration',
                    })
                else:
                    return OrjsonResponse({
                        '_alert': 'Посетитель не найден',
                    })
            else:
                return err(404, 'Событие не найдено')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_qr_create(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_qr_create']):
            file = request.params['method'] + '-' + str(request.params['item_id'])
            data = {
                'type': request.params['type'],
                'method': request.params['method'],
                'id': request.params['item_id']
            }
            url = create_code(request.params['type'], file, data)
            return OrjsonResponse({
                'url': url,
            })
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')
