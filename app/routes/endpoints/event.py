import asyncio
from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.event import Event, find_closest_event



def routes():
    return [
        Route('/event/feed', events_feed, methods = [ 'POST' ]),

        Route('/m/event/list', moderator_event_list, methods = [ 'POST' ]),
        Route('/m/event/update', moderator_event_update, methods = [ 'POST' ]),
        Route('/m/event/create', moderator_event_create, methods = [ 'POST' ]),
    ]



MODELS = {
    'events_feed': {
        'from': {
            'required': True,
            'type': 'int',
            'null': True,
        },
        'to': {
            'required': True,
            'type': 'int',
            'null': True,
        },
        'find': {
            'required': False,
            'type': 'bool',
            'default': False,
        },
    },
    # moderator
	'moderator_event_list': {
        'page': {
            'required': True,
            'type': 'int',
            'value_min': 1,
            'default': 1,
        },
        'reverse': {
            'required': True,
            'type': 'bool',
            'default': False,
        },
	},
	'moderator_event_update': {
		'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'active': {
			'required': True,
			'type': 'bool',
		},
		'name': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'format': {
			'required': True,
			'type': 'str',
            'values': [ 'forum', 'breakfast', 'webinar', 'club', 'meeting', 'education' ],
		},
		'place': {
			'required': True,
			'type': 'str',
		},
		'time_event': {
			'required': True,
			'type': 'int',
            'null': True,
		},
		'detail': {
			'required': True,
			'type': 'str',
		},
	},
	'moderator_event_create': {
        'active': {
			'required': True,
			'type': 'bool',
		},
		'name': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'format': {
			'required': True,
			'type': 'str',
            'values': [ 'forum', 'breakfast', 'webinar', 'club', 'meeting', 'education' ],
		},
		'place': {
			'required': True,
			'type': 'str',
		},
		'time_event': {
			'required': True,
			'type': 'int',
            'null': True,
		},
		'detail': {
			'required': True,
			'type': 'str',
		},
	},
}



################################################################
async def events_feed(request):
    if request.user.id:
        if validate(request.params, MODELS['events_feed']):
            result = await Event.list(
                active_only = True,
                start = request.params['from'],
                finish = request.params['to'],
            )
            #result = result[0:50]
            time_event = None
            if not result and request.params['find']:
                time_event = await find_closest_event(request.params['to'])
            events_ids = [ item.id for item in result ]
            events_ids_selected = await request.user.filter_selected_events(events_ids)
            events_ids_thumbsup = await request.user.filter_thumbsup(events_ids)
            return OrjsonResponse({
                'events': [ item.show() for item in result ],
                'events_selected': { str(id): True for id in events_ids_selected },
                'events_thumbsup': { str(id): True for id in events_ids_thumbsup },
                'closest_time_event': time_event,
            })
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_event_list(request):
    if request.user.id and request.user.check_roles({ 'admin', 'editor', 'manager', 'community manager' }):
        if validate(request.params, MODELS['moderator_event_list']):
            result = await Event.list(reverse = request.params['reverse'])
            i = (request.params['page'] - 1) * 10
            return OrjsonResponse({
                'events': [ item.show() for item in result[i:i + 10] ],
                'amount': len(result),
            })
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_event_update(request):
    if request.user.id and request.user.check_roles({ 'admin', 'editor', 'manager' }):
        if validate(request.params, MODELS['moderator_event_update']):
            event = Event()
            await event.set(id = request.params['id'])
            if event.id:
                await event.update(**request.params)
                dispatch('event_update', request)
                return OrjsonResponse({})
            else:
                return err(404, 'Группа не найдена')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_event_create(request):
    if request.user.id and request.user.check_roles({ 'admin', 'editor', 'manager' }):
        if validate(request.params, MODELS['moderator_event_create']):
            event = Event()
            await event.create(**request.params)
            dispatch('event_create', request)
            return OrjsonResponse({})
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')
