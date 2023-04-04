import asyncio
from random import randint
from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.user import User
from app.models.event import Event
from app.models.item import Item



def routes():
    return [
        Route('/user/{id:int}/info', user_info, methods = [ 'POST' ]),
        Route('/user/summary', user_summary, methods = [ 'POST' ]),
        Route('/user/contacts', user_contacts, methods = [ 'POST' ]),
        Route('/user/search', user_search, methods = [ 'POST' ]),
        Route('/user/contact/add', user_add_contact, methods = [ 'POST' ]),
        Route('/user/contact/del', user_del_contact, methods = [ 'POST' ]),
        Route('/user/event/add', user_add_event, methods = [ 'POST' ]),
        Route('/user/event/del', user_del_event, methods = [ 'POST' ]),
        Route('/user/thumbsup', user_thumbs_up, methods = [ 'POST' ]),
    ]



MODELS = {
	'user_info': {
		'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
	'user_search': {
		'text': {
			'required': True,
			'type': 'str',
            'length_min': 2,
		},
	},
	'user_add_contact': {
		'contact_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
	'user_del_contact': {
		'contact_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
	'user_add_event': {
		'event_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
	'user_del_event': {
		'event_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
	'user_thumbs_up': {
		'item_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
}



################################################################
async def user_info(request):
    if request.user.id:
        if validate(request.params, MODELS['user_search']):
            user = User()
            await user.set(id = request.path_params['id'])
            if user.id:
                return OrjsonResponse(user.show())
            else:
                return err(404, 'Пользователь не найден')
        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_summary(request):
    if request.user.id:
        result = await request.user.get_summary()
        result['amounts_messages'] = await request.user.get_unread_messages_amount()
        return OrjsonResponse(result)
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_contacts(request):
    if request.user.id:
        result = await request.user.get_contacts()
        return OrjsonResponse({ 'contacts': result })
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_search(request):
    if request.user.id:
        if validate(request.params, MODELS['user_search']):
            result = await User.search(text = request.params['text'])
            contacts = await request.user.get_contacts()
            return OrjsonResponse({
                'persons': [ item.show() for item in result ],
                'contacts_cache': { str(contact['id']): True for contact in contacts if contact['type'] == 'person' }
            })
        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_add_contact(request):
    if request.user.id:
        if validate(request.params, MODELS['user_add_contact']):
            user = User()
            await user.set(id = request.params['contact_id'])
            if user.id:
                await request.user.add_contact(user.id)
                dispatch('user_add_contact', request)
                return OrjsonResponse({})
            else:
                return err(404, 'Контакт не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_del_contact(request):
    if request.user.id:
        if validate(request.params, MODELS['user_del_contact']):
            user = User()
            await user.set(id = request.params['contact_id'])
            if user.id:
                await request.user.del_contact(user.id)
                dispatch('user_del_contact', request)
                return OrjsonResponse({})
            else:
                return err(404, 'Контакт не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_add_event(request):
    if request.user.id:
        if validate(request.params, MODELS['user_add_event']):
            event = Event()
            await event.set(id = request.params['event_id'])
            if event.id:
                await request.user.add_event(event.id)
                dispatch('user_add_event', request)
                return OrjsonResponse({})
            else:
                return err(404, 'Событие не найдено')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_del_event(request):
    if request.user.id:
        if validate(request.params, MODELS['user_del_event']):
            event = Event()
            await event.set(id = request.params['event_id'])
            if event.id:
                await request.user.del_event(event.id)
                dispatch('user_del_event', request)
                return OrjsonResponse({})
            else:
                return err(404, 'Событие не найдено')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_thumbs_up(request):
    if request.user.id:
        if validate(request.params, MODELS['user_thumbs_up']):
            item = Item()
            await item.set(id = request.params['item_id'])
            if item.id:
                await request.user.thumbsup(item.id)
                dispatch('user_thumbs_up', request)
                return OrjsonResponse({})
            else:
                return err(404, 'Объект не найдено')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')
