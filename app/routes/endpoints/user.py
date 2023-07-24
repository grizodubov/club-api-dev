from datetime import datetime
import asyncio
from random import randint
from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.user import User, get_residents, get_residents_contacts
from app.models.event import Event
from app.models.item import Item



def routes():
    return [
        Route('/user/{id:int}/info', user_info, methods = [ 'POST' ]),
        Route('/user/update', user_update, methods = [ 'POST' ]),
        Route('/user/summary', user_summary, methods = [ 'POST' ]),
        Route('/user/contacts', user_contacts, methods = [ 'POST' ]),
        Route('/user/recommendations', user_recommendations, methods = [ 'POST' ]),
        Route('/user/suggestions', user_suggestions, methods = [ 'POST' ]),
        Route('/user/suggestions/stats', user_suggestions_stats, methods = [ 'POST' ]),
        Route('/user/search', user_search, methods = [ 'POST' ]),
        Route('/user/contact/add', user_add_contact, methods = [ 'POST' ]),
        Route('/user/contact/del', user_del_contact, methods = [ 'POST' ]),
        Route('/user/event/add', user_add_event, methods = [ 'POST' ]),
        Route('/user/event/del', user_del_event, methods = [ 'POST' ]),
        Route('/user/thumbsup', user_thumbs_up, methods = [ 'POST' ]),

        Route('/m/user/search', moderator_user_search, methods = [ 'POST' ]),
        Route('/m/user/update', moderator_user_update, methods = [ 'POST' ]),
        Route('/m/user/create', moderator_user_create, methods = [ 'POST' ]),

        Route('/new/user/residents', user_residents, methods = [ 'POST' ]),
        Route('/new/user/{id:int}/info', new_user_info, methods = [ 'POST' ]),
        Route('/new/user/update', new_user_update, methods = [ 'POST' ]),

        Route('/new/m/user/update', new_moderator_user_update, methods = [ 'POST' ]),
        Route('/new/m/user/create', new_moderator_user_create, methods = [ 'POST' ]),
    ]



MODELS = {
	'user_info': {
		'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
	'user_update': {
		'name': {
			'required': True,
			'type': 'str',
            'length_min': 2,
		},
		'company': {
			'required': True,
			'type': 'str',
		},
		'position': {
			'required': True,
			'type': 'str',
		},
		'detail': {
			'required': True,
			'type': 'str',
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
	'user_search': {
		'text': {
			'required': True,
			'type': 'str',
            'length_min': 2,
		},
        'reverse': {
			'required': True,
			'type': 'bool',
            'default': False,
		},
	},
	'user_suggestions': {
		'id': {
			'required': True,
			'type': 'int',
            'null': True,
		},
		'filter': {
			'required': True,
			'type': 'str',
            'values': [ 'tags', 'interests' ],
            'null': True,
		},
		'today': {
			'required': True,
			'type': 'bool',
            'default': False,
		},
        'from_id': {
            'required': True,
            'type': 'int',
            'value_min': 1,
            'null': True,
        }
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
    # moderator
	'moderator_user_search': {
		'text': {
			'required': True,
			'type': 'str',
		},
        'applicant': {
            'required': True,
            'type': 'bool',
            'default': False,
            'null': True,
        },
        'page': {
            'required': True,
            'type': 'int',
            'value_min': 1,
            'default': 1,
        },
	},
	'moderator_user_update': {
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
		'phone': {
			'required': True,
			'type': 'str',
            'pattern': r'^[0-9\(\)\-\+\s]{10,20}$',
            'processing': lambda x: x.strip(),
		},
		'email': {
			'required': True,
			'type': 'str',
            'pattern': r'^[^@]+@[^@]+\.[^@]+$',
            'processing': lambda x: x.strip().lower(),
		},
		'company': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'position': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'password': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'status': {
			'required': True,
			'type': 'str',
            'values': [ 'бронзовый', 'серебряный', 'золотой' ],
		},
		'detail': {
			'required': True,
			'type': 'str',
		},
		'roles': {
			'required': True,
			'type': 'str',
            'list': True,
            'values': [ 'client', 'manager', 'moderator', 'editor' ],
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
	'moderator_user_create': {
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
		'phone': {
			'required': True,
			'type': 'str',
            'pattern': r'^[0-9\(\)\-\+\s]{10,20}$',
            'processing': lambda x: x.strip(),
		},
		'email': {
			'required': True,
			'type': 'str',
            'pattern': r'^[^@]+@[^@]+\.[^@]+$',
            'processing': lambda x: x.strip().lower(),
		},
		'company': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'position': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'password': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'status': {
			'required': True,
			'type': 'str',
            'values': [ 'бронзовый', 'серебряный', 'золотой' ],
		},
		'detail': {
			'required': True,
			'type': 'str',
		},
		'roles': {
			'required': True,
			'type': 'str',
            'list': True,
            'values': [ 'client', 'manager' ],
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
    # new
	'new_user_info': {
		'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
	'new_user_update': {
		'name': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'company': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'position': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'annual': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'annual_privacy': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'employees': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'employees_privacy': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'catalog': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'tags': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'interests': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'city': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'hobby': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'birthdate': {
			'required': True,
			'type': 'str',
            'processing': lambda x: datetime.strptime(x.strip(), "%d/%m/%Y"),
            'null': True,
		},
		'birthdate_privacy': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'experience': {
			'required': True,
			'type': 'int',
            'null': True,
		},
		'detail': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
	},
    # new
	'new_moderator_user_update': {
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
		'phone': {
			'required': True,
			'type': 'str',
            'pattern': r'^[0-9\(\)\-\+\s]{10,20}$',
            'processing': lambda x: x.strip(),
		},
		'email': {
			'required': True,
			'type': 'str',
            'pattern': r'^[^@]+@[^@]+\.[^@]+$',
            'processing': lambda x: x.strip().lower(),
		},
		'company': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'position': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'catalog': {
			'required': True,
			'type': 'str',
		},
		'city': {
			'required': True,
			'type': 'str',
		},
		'hobby': {
			'required': True,
			'type': 'str',
		},
		'annual': {
			'required': True,
			'type': 'str',
		},
		'annual_privacy': {
			'required': True,
			'type': 'str',
		},
		'employees': {
			'required': True,
			'type': 'str',
		},
		'employees_privacy': {
			'required': True,
			'type': 'str',
		},
        'password': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'status': {
			'required': True,
			'type': 'str',
            'values': [ 'бронзовый', 'серебряный', 'золотой' ],
		},
		'detail': {
			'required': True,
			'type': 'str',
		},
		'roles': {
			'required': True,
			'type': 'str',
            'list': True,
            'values': [ 'admin', 'client', 'manager', 'moderator', 'editor' ],
		},
		'tags': {
			'required': True,
			'type': 'str',
		},
		'interests': {
			'required': True,
			'type': 'str',
		},
		'birthdate': {
			'required': True,
			'type': 'str',
            'processing': lambda x: datetime.strptime(x.strip(), "%d/%m/%Y"),
            'null': True,
		},
		'birthdate_privacy': {
			'required': True,
			'type': 'str',
		},
		'experience': {
			'required': True,
			'type': 'int',
            'null': True,
		},
	},
	'new_moderator_user_create': {
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
		'phone': {
			'required': True,
			'type': 'str',
            'pattern': r'^[0-9\(\)\-\+\s]{10,20}$',
            'processing': lambda x: x.strip(),
		},
		'email': {
			'required': True,
			'type': 'str',
            'pattern': r'^[^@]+@[^@]+\.[^@]+$',
            'processing': lambda x: x.strip().lower(),
		},
		'company': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'position': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
		'catalog': {
			'required': True,
			'type': 'str',
		},
		'city': {
			'required': True,
			'type': 'str',
		},
		'hobby': {
			'required': True,
			'type': 'str',
		},
		'annual': {
			'required': True,
			'type': 'str',
		},
		'annual_privacy': {
			'required': True,
			'type': 'str',
		},
		'employees': {
			'required': True,
			'type': 'str',
		},
		'employees_privacy': {
			'required': True,
			'type': 'str',
		},
		'password': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'status': {
			'required': True,
			'type': 'str',
            'values': [ 'бронзовый', 'серебряный', 'золотой' ],
		},
		'detail': {
			'required': True,
			'type': 'str',
		},
		'roles': {
			'required': True,
			'type': 'str',
            'list': True,
            'values': [ 'client', 'manager' ],
		},
		'tags': {
			'required': True,
			'type': 'str',
		},
		'interests': {
			'required': True,
			'type': 'str',
		},
		'birthdate': {
			'required': True,
			'type': 'str',
            'processing': lambda x: datetime.strptime(x.strip(), "%d/%m/%Y"),
            'null': True,
		},
		'birthdate_privacy': {
			'required': True,
			'type': 'str',
		},
		'experience': {
			'required': True,
			'type': 'int',
            'null': True,
		},
	},
}



################################################################
async def user_info(request):
    if request.user.id:
        if validate(request.path_params, MODELS['user_info']):
            user = User()
            await user.set(id = request.path_params['id'])
            if user.id:
                result = user.show()
                result.update({
                    'contacts_cache': False,
                    'allow_contact': False,
                })
                contacts = await request.user.get_contacts()
                for contact in contacts:
                    if contact['id'] == user.id:
                        result['contacts_cache'] = True
                        break
                result['allow_contact'] = await request.user.check_access(user)
                return OrjsonResponse(result)
            else:
                return err(404, 'Пользователь не найден')
        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_update(request):
    if request.user.id:
        if validate(request.params, MODELS['user_update']):
            await request.user.update(**request.params)
            dispatch('user_update', request)
            return OrjsonResponse({})
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
async def user_recommendations(request):
    if request.user.id:
        result = await request.user.get_recommendations()
        return OrjsonResponse(result)
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_suggestions(request):
    if request.user.id:
        if validate(request.params, MODELS['user_suggestions']):
            result = await request.user.get_suggestions(
                id = request.params['id'],
                filter = request.params['filter'],
                today = request.params['today'],
                from_id = request.params['from_id'],
            )
            return OrjsonResponse({ 'suggestions': result })
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_suggestions_stats(request):
    if request.user.id:
        result = await request.user.get_suggestions(
            id = None,
            filter = None,
            today = True,
        )
        stats = {
            'bid': 0,
            'ask': 0,
        }
        for item in result:
            if item['offer'] == 'bid':
                stats['bid'] += 1
            else:
                stats['ask'] += 1
        return OrjsonResponse(stats)
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_search(request):
    if request.user.id:
        if validate(request.params, MODELS['user_search']):
            result = await User.search(
                text = request.params['text'],
                reverse = request.params['reverse'],
                offset = 0,
                limit = 50,
            )
            contacts = await request.user.get_contacts()
            allow_contacts = {}
            if result:
                allow_contacts = await request.user.check_multiple_access([ item for item in result if item.id != request.user.id ])
            return OrjsonResponse({
                'persons': [ item.show() for item in result if item.id != request.user.id ],
                'contacts_cache': { str(contact['id']): True for contact in contacts if contact['type'] == 'person' },
                'allow_contacts': allow_contacts,
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
            if request.user.id == user.id:
                return err(400, 'Неверный запрос')
            if user.id:
                access = await request.user.check_access(user)
                if access:
                    await request.user.add_contact(user.id)
                    dispatch('user_add_contact', request)
                    return OrjsonResponse({})
                else:
                    return err(403, 'Нет доступа')
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



################################################################
async def moderator_user_search(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator' }):
        if validate(request.params, MODELS['moderator_user_search']):
            (result, amount) = await User.search(
                text = request.params['text'],
                active_only = False,
                offset = (request.params['page'] - 1) * 10,
                limit = 10,
                count = True,
                applicant = request.params['applicant'] if request.params['applicant'] is not None else False,
            )
            return OrjsonResponse({
                'users': [ item.dump() for item in result ],
                'amount': amount,
            })
        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_user_update(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator' }):
        if validate(request.params, MODELS['moderator_user_update']):
            user = User()
            await user.set(id = request.params['id'], active = None)
            if user.id:
                temp = User()
                if await temp.find(email = request.params['email']):
                    if temp.id != user.id:
                        return err(400, 'Email уже зарегистрирован')
                temp = User()
                if await temp.find(phone = request.params['phone']):
                    if temp.id != user.id:
                        return err(400, 'Телефон уже зарегистрирован')
                await user.update(**request.params)
                dispatch('user_update', request)
                return OrjsonResponse({})
            else:
                return err(404, 'Пользователь не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_user_create(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator' }):
        if validate(request.params, MODELS['moderator_user_create']):
            user = User()
            if await user.find(email = request.params['email']):
                return err(400, 'Email уже зарегистрирован')
            if await user.find(phone = request.params['phone']):
                return err(400, 'Телефон уже зарегистрирован')
            await user.create(
                name = request.params['name'],
                email = request.params['email'],
                phone = request.params['phone'],
                company = request.params['company'],
                position = request.params['position'],
                password = request.params['password'],
                roles = request.params['roles'],
                active = request.params['active'],
                detail = request.params['detail'],
                status = request.params['status'],
                tags = request.params['tags'],
                interests = request.params['interests'],
            )
            dispatch('user_create', request)
            return OrjsonResponse({})
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_residents(request):
    if request.user.id:
        result = await get_residents()
        contacts = await get_residents_contacts(
            user_id = request.user.id,
            user_status = request.user.status,
            contacts_ids = [ item.id for item in result ]
        )
        return OrjsonResponse({
            'residents': [ item.show() for item in result ],
            'contacts': contacts,
        })
    else:
        return err(403, 'Нет доступа')



################################################################
async def new_user_info(request):
    if request.user.id:
        if validate(request.path_params, MODELS['user_info']):
            user = User()
            await user.set(id = request.path_params['id'])
            if user.id:
                result = {}
                if user.id == request.user.id:
                    result = user.dshow()
                    result.update({ 
                        'contact': False,
                        'allow_contact': False
                    })
                else:
                    result = user.show()
                    contact = await get_residents_contacts(
                        user_id = request.user.id,
                        user_status = request.user.status,
                        contacts_ids = [ user.id ]
                    )
                    result.update(contact[str(user.id)])
                return OrjsonResponse(result)
            else:
                return err(404, 'Пользователь не найден')
        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')



################################################################
async def new_user_update(request):
    if request.user.id:
        if validate(request.params, MODELS['new_user_update']):
            await request.user.update(**request.params)
            dispatch('user_update', request)
            user = User()
            await user.set(id = request.user.id)
            result = user.dshow()
            result.update({ 
                'contact': False,
                'allow_contact': False
            })
            return OrjsonResponse(result)
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def new_moderator_user_update(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator' }):
        if validate(request.params, MODELS['new_moderator_user_update']):
            user = User()
            await user.set(id = request.params['id'], active = None)
            if user.id:
                temp = User()
                if await temp.find(email = request.params['email']):
                    if temp.id != user.id:
                        return err(400, 'Email уже зарегистрирован')
                temp = User()
                if await temp.find(phone = request.params['phone']):
                    if temp.id != user.id:
                        return err(400, 'Телефон уже зарегистрирован')
                if 'admin' in request.params['roles']:
                    if user.id not in { 8000, 10004 }:
                        return err(400, 'Неверный запрос')
                await user.update(**request.params)
                dispatch('user_update', request)
                return OrjsonResponse({})
            else:
                return err(404, 'Пользователь не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def new_moderator_user_create(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator' }):
        if validate(request.params, MODELS['new_moderator_user_create']):
            user = User()
            if await user.find(email = request.params['email']):
                return err(400, 'Email уже зарегистрирован')
            if await user.find(phone = request.params['phone']):
                return err(400, 'Телефон уже зарегистрирован')
            if 'admin' in request.params['roles']:
                if user.id not in { 8000, 10004 }:
                    return err(400, 'Неверный запрос')
            await user.create(
                name = request.params['name'],
                email = request.params['email'],
                phone = request.params['phone'],
                company = request.params['company'],
                position = request.params['position'],
                catalog = request.params['catalog'],
                password = request.params['password'],
                roles = request.params['roles'],
                active = request.params['active'],
                detail = request.params['detail'],
                status = request.params['status'],
                city = request.params['city'],
                hobby = request.params['hobby'],
                tags = request.params['tags'],
                interests = request.params['interests'],
                annual = request.params['annual'],
                annual_privacy = request.params['annual_privacy'],
                employees = request.params['employees'],
                employees_privacy = request.params['employees_privacy'],
                birthdate = request.params['birthdate'],
                birthdate_privacy = request.params['birthdate_privacy'],
                experience = request.params['experience'],
            )
            dispatch('user_create', request)
            return OrjsonResponse({})
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')
