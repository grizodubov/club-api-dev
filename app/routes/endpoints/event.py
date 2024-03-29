import asyncio
import orjson
from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.event import Event, find_closest_event, get_participants, get_participants_with_avatars, get_all_speakers, get_future_events
from app.models.user import User



def routes():
    return [
        Route('/event/feed', events_feed, methods = [ 'POST' ]),
        Route('/event/info', event_info, methods = [ 'POST' ]),

        Route('/m/event/list', moderator_event_list, methods = [ 'POST' ]),
        Route('/m/event/update', moderator_event_update, methods = [ 'POST' ]),
        Route('/m/event/create', moderator_event_create, methods = [ 'POST' ]),
        Route('/m/event/patch', moderator_event_patch, methods = [ 'POST' ]),

        Route('/m/event/speaker/add', moderator_event_speaker_add, methods = [ 'POST' ]),
        Route('/m/event/speaker/delete', moderator_event_speaker_delete, methods = [ 'POST' ]),
        Route('/m/event/program/update', moderator_event_program_update, methods = [ 'POST' ]),
        Route('/m/event/user/add', moderator_event_user_add, methods = [ 'POST' ]),
        Route('/m/event/user/del', moderator_event_user_del, methods = [ 'POST' ]),

        Route('/ma/event/user/list', manager_event_user_list, methods = [ 'POST' ]),
        Route('/ma/event/list', manager_event_list, methods = [ 'POST' ]),
        Route('/ma/event/user/tags/update', manager_event_user_tags_update, methods = [ 'POST' ]),
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
    'event_info': {
        'id': {
            'required': True,
            'type': 'int',
            'value_min': 1,
        },
        'suggestions': {
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
            'values': [ 'forum', 'breakfast', 'webinar', 'club', 'meeting', 'education', 'tender', 'place', 'network', 'expert', 'mission', 'guest' ],
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
            'values': [ 'forum', 'breakfast', 'webinar', 'club', 'meeting', 'education', 'tender', 'place', 'network', 'expert', 'mission', 'guest' ],
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
	'moderator_event_patch': {
		'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'patch': {
			'required': True,
			'type': 'str',
            'length_min': 3,
		},
	},

	'moderator_event_speaker_add': {
		'event_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'user_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
	'moderator_event_speaker_delete': {
		'event_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'user_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
    'moderator_event_program_update': {
        'event_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'speakers': {
            'required': True,
			'type': 'int',
            'value_min': 1,
            'list': True,
            'null': True,
        },
        'program': {
            'required': True,
            'type': 'dict',
            'list': True,
            'scheme': {
                'name': {
                    'required': True,
                    'type': 'str',
                },
                'date': {
                    'required': True,
                    'type': 'int',
                },
                'time': {
                    'required': True,
                    'type': 'str',
                },
                'speakers': {
                    'required': True,
                    'type': 'int',
                    'list': True,
                    'null': True,
                },
            },
        }
    },
	'moderator_event_user_add': {
		'event_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'user_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
	'moderator_event_user_del': {
		'event_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'user_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
    # manager
    'manager_event_user_list': {
        'user_id': {
            'required': True,
            'type': 'int',
            'value_min': 1,
        },
        'archive': {
            'required': True,
            'type': 'bool',
        },
    },
    'manager_event_user_tags_update': {
        'user_id': {
            'required': True,
            'type': 'int',
            'value_min': 1,
        },
        'event_id': {
            'required': True,
            'type': 'int',
            'value_min': 1,
        },
        'tags': {
			'required': True,
			'type': 'str',
		},
        'interests': {
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
            events_selected = await request.user.filter_selected_events(events_ids)
            events_ids_thumbsup = await request.user.filter_thumbsup(events_ids)
            events = []
            participants = await get_participants(events_ids = events_ids)
            for event in result:
                k = str(event.id)
                event_participants = { 'participants': participants[k] if k in participants else [] }
                events.append(
                    event.show() | event_participants
                )
            return OrjsonResponse({
                'events': events,
                'events_selected': { str(item['event_id']): item['confirmation'] for item in events_selected },
                'events_thumbsup': { str(id): True for item in events_ids_thumbsup },
                'closest_time_event': time_event,
            })
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def event_info(request):
    if request.user.id:
        if validate(request.params, MODELS['event_info']):
            event = Event()
            await event.set(id = request.params['id'])
            if event.id:
                info = await event.info()
                participants = await get_participants(events_ids = [ event.id ])
                event_participants = participants[str(event.id)] if str(event.id) in participants else []
                users = info['speakers'] + event_participants
                residents = []
                if users:
                    result = await User.search(text = '')
                    ### remove data for roles
                    roles = set(request.user.roles)
                    roles.discard('applicant')
                    roles.discard('guest')
                    for item in result:
                        temp = item.show()
                        if not roles and request.user.id != temp['id']:
                            temp['company'] = ''
                            temp['position'] = ''
                            temp['link_telegram'] = ''
                        residents.append(temp)
                suggestions = await request.user.get_event_suggestions(
                    event_id = event.id,
                    users_ids = [ p['id'] for p in event_participants ],
                )
                return OrjsonResponse({
                    'event': info | { 'participants': event_participants, 'suggestions': suggestions },
                    'residents': residents,
                })
            else:
                return err(404, 'Событие не найдено')
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
            events = []
            events_ids = [ event.id for event in result[i:i + 10] ]
            participants = await get_participants(events_ids = events_ids)
            for event in result[i:i + 10]:
                k = str(event.id)
                event_participants = { 'participants': participants[k] if k in participants else [] }
                events.append(
                    event.show() | event.get_patch() | event_participants
                )
            speakers = await get_all_speakers()
            return OrjsonResponse({
                'events': events,
                'amount': len(result),
                'speakers': speakers,
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



################################################################
async def moderator_event_patch(request):
    if request.user.id and request.user.check_roles({ 'admin', 'editor', 'manager' }):
        if validate(request.params, MODELS['moderator_event_patch']):
            event = Event()
            await event.set(id = request.params['id'])
            if event.id:
                data = None
                try:
                    data = orjson.loads(request.params['patch'])
                except:
                    data = None
                if data:
                    event.set_patch(data)
                    dispatch('event_update', request)
                    return OrjsonResponse({})
                else:
                    return err(400, 'Неверный запрос')
            else:
                return err(404, 'Событие не найдено')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_event_speaker_add(request):
    if request.user.id and request.user.check_roles({ 'admin', 'editor', 'manager' }):
        if validate(request.params, MODELS['moderator_event_speaker_add']):
            event = Event()
            await event.set(id = request.params['event_id'])
            if event.id:
                user = User()
                await user.set(id = request.params['event_id'])
                if user.id:
                    await event.add_speaker(user_id = user.id)
                    dispatch('event_update', request)
                    return OrjsonResponse({})
                else:
                    return err(404, 'Спикер не найден')
            else:
                return err(404, 'Событие не найдено')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_event_speaker_delete(request):
    if request.user.id and request.user.check_roles({ 'admin', 'editor', 'manager' }):
        if validate(request.params, MODELS['moderator_event_speaker_delete']):
            event = Event()
            await event.set(id = request.params['event_id'])
            if event.id:
                user = User()
                await user.set(id = request.params['event_id'])
                if user.id:
                    await event.delete_speaker(user_id = user.id)
                    dispatch('event_update', request)
                    return OrjsonResponse({})
                else:
                    return err(404, 'Спикер не найден')
            else:
                return err(404, 'Событие не найдено')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_event_program_update(request):
    if request.user.id and request.user.check_roles({ 'admin', 'editor', 'manager' }):
        if validate(request.params, MODELS['moderator_event_program_update']):
            event = Event()
            await event.set(id = request.params['event_id'])
            if event.id:
                await event.update_speakers(request.params['speakers'])
                await event.update_program(request.params['program'])
                dispatch('event_update', request)
                return OrjsonResponse({})
            else:
                return err(404, 'Событие не найдено')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_event_user_add(request):
    if request.user.id:
        if validate(request.params, MODELS['moderator_event_user_add']):
            event = Event()
            await event.set(id = request.params['event_id'])
            if event.id:
                user = User()
                await user.set(id = request.params['user_id'])
                if user.id:
                    await user.add_event(event.id)
                    dispatch('user_add_event', request)
                    return OrjsonResponse({})
                else:
                    return err(404, 'Пользователь не найден')
            else:
                return err(404, 'Событие не найдено')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')
        


################################################################
async def moderator_event_user_del(request):
    if request.user.id:
        if validate(request.params, MODELS['moderator_event_user_del']):
            event = Event()
            await event.set(id = request.params['event_id'])
            if event.id:
                user = User()
                await user.set(id = request.params['user_id'])
                if user.id:
                    await user.del_event(event.id)
                    dispatch('user_add_event', request)
                    return OrjsonResponse({})
                else:
                    return err(404, 'Пользователь не найден')
            else:
                return err(404, 'Событие не найдено')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_event_user_list(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager', 'agent' }):
        if validate(request.params, MODELS['manager_event_user_list']):
            user = User()
            await user.set(id = request.params['user_id'], active = None)
            if user.id:
                if request.params['archive']:
                    events = await user.get_events_archive()
                else:
                    events = await user.get_events()
                return OrjsonResponse({
                    'events': events,
                })
            else:
                return err(404, 'Пользователь не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_event_list(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        events = await get_future_events()
        ids = [ event['id'] for event in events ]
        clients_ids = await request.user.get_allowed_clients_ids()
        participants = {}
        if ids:
            participants = await get_participants_with_avatars(ids)
        participants_filtered = {}
        if clients_ids is None:
            participants_filtered = participants
        else:
            if participants:
                for k, v in participants.items():
                    if v:
                        temp = [ u['id'] for u in v if u['id'] in clients_ids ]
                        if temp:
                            participants_filtered[k] = temp
        return OrjsonResponse({
            'events': events,
            'participants': participants_filtered,
        })
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_event_user_tags_update(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_event_user_tags_update']):
            user = User()
            await user.set(id = request.params['user_id'], active = None)
            if user.id:
                if request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief' }) or user.community_manager_id == request.user.id:
                    event = Event()
                    await event.set(id = request.params['event_id'])
                    if event.id:
                        await user.update_event_tags(event_id = event.id, tags = request.params['tags'], interests = request.params['interests'])
                        dispatch('event_update', request)
                        return OrjsonResponse({})
                    else:
                        return err(404, 'Событие не найдено')
                else:
                    return err(403, 'Нет доступа')
            else:
                return err(404, 'Пользователь не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')
