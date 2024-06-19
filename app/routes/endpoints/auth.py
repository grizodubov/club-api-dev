import asyncio
import orjson
from datetime import datetime
from random import randint
from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.user import User, validate_registration, validate_registration_new
from app.models.session import check_by_token
from app.helpers.email import send_email
from app.helpers.mobile import send_mobile_message
from app.helpers.templates import VERIFICATION_CODE, CHANGE_EMAIL_CODE, CHANGE_MOBILE_CODE
from app.models.item import Item
from app.models.event import Event



def routes():
    return [
        Route('/login/mobile', login_mobile, methods = [ 'POST' ]),
        Route('/login/mobile/validate', login_mobile_validate, methods = [ 'POST' ]),
        Route('/login/email', login_email, methods = [ 'POST' ]),
        Route('/login/email/validate', login_email_validate, methods = [ 'POST' ]),
        Route('/login', login, methods = [ 'POST' ]),
        Route('/logout', logout, methods = [ 'POST' ]),
        Route('/check/token', check_token, methods = [ 'POST' ]),
        Route('/check/avatar/upload', check_avatar_upload, methods = [ 'POST' ]),
        Route('/check/event/upload', check_event_upload, methods = [ 'POST' ]),
        Route('/register', register, methods = [ 'POST' ]),
        Route('/register/validate', register_validate, methods = [ 'POST' ]),
        Route('/terminate', terminate, methods = [ 'POST' ]),

        Route('/m/login', moderator_login, methods = [ 'POST' ]),
        Route('/m/login/mobile/validate', moderator_login_mobile_validate, methods = [ 'POST' ]),
        Route('/m/login/email/validate', moderator_login_email_validate, methods = [ 'POST' ]),

        Route('/new/validate', new_validate, methods = [ 'POST' ]),
        Route('/new/register', new_register, methods = [ 'POST' ]),

        Route('/change/email', change_email, methods = [ 'POST' ]),
        Route('/change/email/validate', change_email_validate, methods = [ 'POST' ]),
        Route('/change/mobile', change_mobile, methods = [ 'POST' ]),
        Route('/change/mobile/validate', change_mobile_validate, methods = [ 'POST' ]),
        Route('/change/credentials', change_credentials, methods = [ 'POST' ]),

        Route('/man/login/mobile/validate', manager_login_mobile_validate, methods = [ 'POST' ]),
        Route('/man/login/email/validate', manager_login_email_validate, methods = [ 'POST' ]),
        Route('/man/login', manager_login, methods = [ 'POST' ]),

        Route('/register/device', register_device, methods = [ 'POST' ]),
    ]



MODELS = {
	'login': {
		'account': {
			'required': True,
			'type': 'str',
            'length_min': 4,
            'processing': lambda x: x.strip().lower(),
		},
		'password': {
			'required': True,
			'type': 'str',
            'length_min': 4,
            'processing': lambda x: x.strip(),
		},
	},
	'login_mobile': {
		'account': {
			'required': True,
			'type': 'str',
            'length_min': 4,
            'processing': lambda x: x.strip().lower(),
		},
	},
	'login_mobile_validate': {
		'account': {
			'required': True,
			'type': 'str',
            'length_min': 4,
            'processing': lambda x: x.strip().lower(),
		},
		'code': {
			'required': True,
			'type': 'str',
            'length': 4,
            'pattern': r'^\d{4}$',
		},
	},
	'login_email': {
		'account': {
			'required': True,
			'type': 'str',
            'length_min': 4,
            'processing': lambda x: x.strip().lower(),
		},
	},
	'login_email_validate': {
		'account': {
			'required': True,
			'type': 'str',
            'length_min': 4,
            'processing': lambda x: x.strip().lower(),
		},
		'code': {
			'required': True,
			'type': 'str',
            'length': 4,
            'pattern': r'^\d{4}$',
		},
	},
	'check_token': {
		'token': {
			'required': True,
			'type': 'str',
            'length': 64,
            'pattern': r'^[0-9A-Fa-f]{64}$',
		},
	},
	'check_avatar_upload': {
		'token': {
			'required': True,
			'type': 'str',
            'length': 64,
            'pattern': r'^[0-9A-Fa-f]{64}$',
		},
        'owner_id': {
            'required': True,
			'type': 'int',
            'value_min': 1,
            'null': True,
        },
	},
	'check_event_upload': {
		'token': {
			'required': True,
			'type': 'str',
            'length': 64,
            'pattern': r'^[0-9A-Fa-f]{64}$',
		},
        'event_id': {
            'required': True,
			'type': 'int',
            'value_min': 1,
        },
	},
	'register': {
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
	},
	'register_validate': {
		'account': {
			'required': True,
			'type': 'str',
            'length_min': 4,
            'processing': lambda x: x.strip().lower(),
		},
		'email_code': {
			'required': True,
			'type': 'str',
            'length': 4,
            'pattern': r'^\d{4}$',
		},
		'phone_code': {
			'required': True,
			'type': 'str',
            'length': 4,
            'pattern': r'^\d{4}$',
		},
	},
	'change_email': {
		'account': {
			'required': True,
			'type': 'str',
            'length_min': 4,
            'processing': lambda x: x.strip().lower(),
		},
	},
	'change_email_validate': {
		'account': {
			'required': True,
			'type': 'str',
            'length_min': 4,
            'processing': lambda x: x.strip().lower(),
		},
		'code': {
			'required': True,
			'type': 'str',
            'length': 4,
            'pattern': r'^\d{4}$',
		},
	},
	'change_mobile': {
		'account': {
			'required': True,
			'type': 'str',
            'length_min': 4,
            'processing': lambda x: x.strip().lower(),
		},
	},
	'change_mobile_validate': {
		'account': {
			'required': True,
			'type': 'str',
            'length_min': 4,
            'processing': lambda x: x.strip().lower(),
		},
		'code': {
			'required': True,
			'type': 'str',
            'length': 4,
            'pattern': r'^\d{4}$',
		},
	},
	'change_credentials': {
		'code': {
			'required': True,
			'type': 'str',
            'length_min': 4,
            'processing': lambda x: x.strip(),
		},
	},
    # moderator
	'moderator_login': {
		'account': {
			'required': True,
			'type': 'str',
            'length_min': 4,
            'processing': lambda x: x.strip().lower(),
		},
		'password': {
			'required': True,
			'type': 'str',
            'length_min': 4,
            'processing': lambda x: x.strip(),
		},
	},
	'moderator_login_mobile_validate': {
		'account': {
			'required': True,
			'type': 'str',
            'length_min': 4,
            'processing': lambda x: x.strip().lower(),
		},
		'code': {
			'required': True,
			'type': 'str',
            'length': 4,
            'pattern': r'^\d{4}$',
		},
	},
	'moderator_login_email_validate': {
		'account': {
			'required': True,
			'type': 'str',
            'length_min': 4,
            'processing': lambda x: x.strip().lower(),
		},
		'code': {
			'required': True,
			'type': 'str',
            'length': 4,
            'pattern': r'^\d{4}$',
		},
	},
    # new
    'new_validate': {
		'email': {
			'required': True,
			'type': 'str',
            'pattern': r'^[^@]+@[^@]+\.[^@]+$',
            'processing': lambda x: x.strip().lower(),
		},
		'phone': {
			'required': True,
			'type': 'str',
            'pattern': r'^[0-9\(\)\-\+\s]{10,20}$',
            'processing': lambda x: x.strip(),
		},
	},
    'new_register': {
		'email': {
			'required': True,
			'type': 'str',
            'pattern': r'^[^@]+@[^@]+\.[^@]+$',
            'processing': lambda x: x.strip().lower(),
		},
		'phone': {
			'required': True,
			'type': 'str',
            'pattern': r'^[0-9\(\)\-\+\s]{10,20}$',
            'processing': lambda x: x.strip(),
		},
		'email_code': {
			'required': True,
			'type': 'str',
            'length': 4,
            'pattern': r'^\d{4}$',
		},
		'phone_code': {
			'required': True,
			'type': 'str',
            'length': 4,
            'pattern': r'^\d{4}$',
		},
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
		'password': {
			'required': True,
			'type': 'str',
            'length_min': 2,
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
	},
    'register_device': {
		'device_id': {
			'required': True,
			'type': 'str',
            'null': True,
		},
		'device_info': {
			'required': True,
			'type': 'str',
            'null': True,
		},
		'device_token': {
			'required': True,
			'type': 'str',
            'null': True,
		},
    },
}



################################################################
async def login(request):
    await asyncio.sleep(.5)
    #print('LOGIN!!!')
    if validate(request.params, MODELS['login']):
        user = User()        
        if await user.check(request.params['account'], request.params['password']):
            await request.session.assign(user.id)
            request.user.copy(user = user)
            request.api.websocket_update(request.session.id, request.user.id)
            dispatch('user_login', request)
            return OrjsonResponse({})
        else:
            return err(403, 'Пользователь и / или пароль не верны')
    else:
        return err(400, 'Не указаны логин и / или пароль')



################################################################
async def login_email(request):
    await asyncio.sleep(.25)
    if validate(request.params, MODELS['login_email']):
        user = User()
        if await user.find(email = request.params['account']) and user.active:
            code = str(randint(1000, 9999))
            await user.set_validation_code(code = code)
            send_email(request.api.stream_email, user.email, VERIFICATION_CODE['subject'], VERIFICATION_CODE['body'], { 'code': code })
            return OrjsonResponse({})
        else:
            return err(404, 'Пользователь не найден')
    else:
        return err(400, 'Не указан email')



################################################################
async def login_email_validate(request):
    await asyncio.sleep(.5)
    if validate(request.params, MODELS['login_email_validate']):
        user = User()
        if await user.find(email = request.params['account']) and user.active:
            if await user.check_validation_code(code = request.params['code']):
                await request.session.assign(user.id)
                request.user.copy(user = user)
                request.api.websocket_update(request.session.id, request.user.id)
                dispatch('user_login', request)
                return OrjsonResponse({})
            else:
                return err(403, 'Код не верен')
        else:
            return err(404, 'Пользователь не найден')
    else:
        return err(400, 'Не указан email')



################################################################
async def login_mobile(request):
    await asyncio.sleep(.25)
    if validate(request.params, MODELS['login_mobile']):
        user = User()
        if await user.find(phone = request.params['account']) and user.active:
            code = str(randint(1000, 9999))
            await user.set_validation_code(code = code)
            send_mobile_message(request.api.stream_mobile, user.phone, VERIFICATION_CODE['message'], { 'code': code })
            return OrjsonResponse({})
        else:
            return err(404, 'Пользователь не найден')
    else:
        return err(400, 'Не указан телефон')



################################################################
async def login_mobile_validate(request):
    await asyncio.sleep(.5)
    if validate(request.params, MODELS['login_mobile_validate']):
        user = User()
        if await user.find(phone = request.params['account']) and user.active:
            if await user.check_validation_code(code = request.params['code']):
                await request.session.assign(user.id)
                request.user.copy(user = user)
                request.api.websocket_update(request.session.id, request.user.id)
                dispatch('user_login', request)
                return OrjsonResponse({})
            else:
                return err(403, 'Код не верен')
        else:
            return err(404, 'Пользователь не найден')
    else:
        return err(400, 'Не указан email')



################################################################
async def logout(request):
    if request.user.id:
        user_id = request.user.id
        await request.session.assign(0)
        request.user.reset()
        request.api.websocket_update(request.session.id, request.user.id)
        dispatch('user_logout', request, user_id)
        return OrjsonResponse({})
    else:
        return err(403, 'Нет доступа')



################################################################
async def check_token(request):
    if validate(request.params, MODELS['check_token']):
        if request.user.id == 1010:
            result = await check_by_token(request.params['token'])
            if result and result['user_id']:
                user = User()
                await user.set(id = result['user_id'])
                return OrjsonResponse({
                    'user_id': result['user_id'],
                    'user_roles': user.roles,
                })
            else:
                return err(404, 'Пользователь не найден')
        else:
            return err(403, 'Нет доступа')
    else:
        return err(400, 'Неверный токен')



################################################################
async def check_avatar_upload(request):
    if validate(request.params, MODELS['check_avatar_upload']):
        if request.user.id == 1010:
            result = await check_by_token(request.params['token'])
            if result and result['user_id']:
                user = User()
                await user.set(id = result['user_id'])
                owner_id = request.params['owner_id']
                if owner_id is None:
                    owner_id = result['user_id']
                item = Item()
                await item.set(id = owner_id)
                if item.id and item.model in { 'user', 'group', 'community' }:
                    if item.model == 'user':
                        owner = User()
                        await owner.set(id = owner_id)
                        if owner.id:
                            if user.id == owner.id or \
                                    (set(user.roles) & { 'community manager' } and owner.community_manager_id == user.id) or \
                                    set(user.roles) & { 'admin', 'manager' }:
                                return OrjsonResponse({
                                    'owner_id': owner_id,
                                    'access': True,
                                })
                            else:
                                return err(403, 'Нет доступа')
                        else:
                            return err(404, 'Пользователь не найден [owner]')
                    else:
                        if set(user.roles) & { 'admin', 'manager' }:
                            return OrjsonResponse({
                                'owner_id': owner_id,
                                'access': True,
                            })
                        else:
                            return err(403, 'Нет доступа')
                else:
                    return err(404, 'Объект не найден [owner]')
            else:
                return err(404, 'Пользователь не найден [initiator]')
        else:
            return err(403, 'Нет доступа')
    else:
        return err(400, 'Неверный токен')



################################################################
async def check_event_upload(request):
    if validate(request.params, MODELS['check_event_upload']):
        if request.user.id == 1010:
            result = await check_by_token(request.params['token'])
            if result and result['user_id']:
                user = User()
                await user.set(id = result['user_id'])
                if user.id and set(user.roles) & { 'admin', 'manager', 'moderator', 'editor' }:
                    event = Event()
                    await event.set(id = request.params['event_id'])
                    if event.id:
                        return OrjsonResponse({
                            'access': True,
                            'event_id': event.id,
                        })
                    else:
                        return err(404, 'Событие не найдено')
                else:
                    return err(403, 'Нет доступа')
            else:
                return err(404, 'Пользователь не найден [initiator]')
        else:
            return err(403, 'Нет доступа')
    else:
        return err(400, 'Неверный токен')



################################################################
async def register(request):
    await asyncio.sleep(.5)
    if validate(request.params, MODELS['register']):
        user = User()
        if await user.find(email = request.params['email']):
            return err(400, 'Email уже зарегистрирован')
        if await user.find(phone = request.params['phone']):
            return err(400, 'Телефон уже зарегистрирован')
        email_code = str(randint(1000, 9999))
        phone_code = str(randint(1000, 9999))
        await user.prepare(
            user_data = {
                'name': request.params['name'],
                'email': request.params['email'],
                'phone': request.params['phone'],
                'company': request.params['company'],
                'position': request.params['position'],
                'password': request.params['password'],
                'roles': [ 10059 ],
            },
            email_code = email_code,
            phone_code = phone_code,
        )
        send_email(request.api.stream_email, request.params['email'], VERIFICATION_CODE['subject'], VERIFICATION_CODE['body'], { 'code': email_code })
        send_mobile_message(request.api.stream_mobile, request.params['phone'], VERIFICATION_CODE['message'], { 'code': phone_code })
        return OrjsonResponse({})
    else:
        return err(400, 'Неверные данные')



################################################################
async def register_validate(request):
    await asyncio.sleep(.5)
    if validate(request.params, MODELS['register_validate']):
        user = User()
        if await user.find(email = request.params['account']):
            return err(400, 'Email уже зарегистрирован')
        user_data = await validate_registration(
            email = request.params['account'],
            email_code = request.params['email_code'],
            phone_code = request.params['phone_code'],
        )
        if user_data:
            if await user.find(phone = user_data['phone']):
                return err(400, 'Телефон уже зарегистрирован')
            await user.create(
                name = user_data['name'],
                email = user_data['email'],
                phone = user_data['phone'],
                company = user_data['company'],
                position = user_data['position'],
                password = user_data['password'],
                roles = user_data['roles'],
            )
            await request.session.assign(user.id)
            request.user.copy(user = user)
            request.api.websocket_update(request.session.id, request.user.id)
            dispatch('user_register', request)
            return OrjsonResponse({})
        else:
            return err(403, 'Проверочный код не верен')
    else:
        return err(400, 'Не указан email')



################################################################
async def moderator_login(request):
    await asyncio.sleep(.5)
    if validate(request.params, MODELS['moderator_login']):
        user = User()        
        if await user.check(request.params['account'], request.params['password']):
            if set(user.roles) & { 'admin', 'moderator', 'editor', 'manager', 'community manager', 'chief' }:
                await request.session.assign(user.id)
                request.user.copy(user = user)
                request.api.websocket_update(request.session.id, request.user.id)
                # dispatch('user_login', request)
                return OrjsonResponse({})
            else:
                return err(403, 'Нет доступа')
        else:
            return err(403, 'Пользователь и / или пароль не верны')
    else:
        return err(400, 'Не указаны логин и / или пароль')



################################################################
async def moderator_login_email_validate(request):
    await asyncio.sleep(.5)
    if validate(request.params, MODELS['moderator_login_email_validate']):
        user = User()
        if await user.find(email = request.params['account']) and user.active:
            if await user.check_validation_code(code = request.params['code']):
                if set(user.roles) & { 'admin', 'moderator', 'editor', 'manager', 'community manager' }:
                    await request.session.assign(user.id)
                    request.user.copy(user = user)
                    request.api.websocket_update(request.session.id, request.user.id)
                    return OrjsonResponse({})
                else:
                    return err(403, 'Нет доступа')
            else:
                return err(403, 'Код не верен')
        else:
            return err(404, 'Пользователь не найден')
    else:
        return err(400, 'Не указан email')



################################################################
async def moderator_login_mobile_validate(request):
    await asyncio.sleep(.5)
    if validate(request.params, MODELS['moderator_login_mobile_validate']):
        user = User()
        if await user.find(phone = request.params['account']) and user.active:
            if await user.check_validation_code(code = request.params['code']):
                if set(user.roles) & { 'admin', 'moderator', 'editor', 'manager', 'community manager' }:
                    await request.session.assign(user.id)
                    request.user.copy(user = user)
                    request.api.websocket_update(request.session.id, request.user.id)
                    return OrjsonResponse({})
                else:
                    return err(403, 'Нет доступа')
            else:
                return err(403, 'Код не верен')
        else:
            return err(404, 'Пользователь не найден')
    else:
        return err(400, 'Не указан email')



################################################################
async def terminate(request):
    if request.user.id:
        await request.user.terminate()
        user_id = request.user.id
        await request.session.assign(0)
        request.user.reset()
        request.api.websocket_update(request.session.id, request.user.id)
        dispatch('user_logout', request, user_id)
        return OrjsonResponse({})
    else:
        return err(403, 'Нет доступа')



################################################################
async def new_validate(request):
    await asyncio.sleep(.5)
    if validate(request.params, MODELS['new_validate']):
        user = User()
        if await user.find(email = request.params['email']):
            return err(400, 'Email уже зарегистрирован')
        if await user.find(phone = request.params['phone']):
            return err(400, 'Телефон уже зарегистрирован')
        email_code = str(randint(1000, 9999))
        phone_code = str(randint(1000, 9999))
        await user.prepare_new(
            user_data = {
                'email': request.params['email'],
                'phone': request.params['phone'],
                'roles': [ 10059 ],
            },
            email_code = email_code,
            phone_code = phone_code,
        )
        print(email_code, phone_code)
        send_email(request.api.stream_email, request.params['email'], VERIFICATION_CODE['subject'], VERIFICATION_CODE['body'], { 'code': email_code })
        send_mobile_message(request.api.stream_mobile, request.params['phone'], VERIFICATION_CODE['message'], { 'code': phone_code })
        return OrjsonResponse({})
    else:
        return err(400, 'Неверные данные')



################################################################
async def new_register(request):
    await asyncio.sleep(.5)
    if validate(request.params, MODELS['new_register']):
        user = User()
        if await user.find(email = request.params['email']):
            return err(400, 'Email уже зарегистрирован')
        if await user.find(phone = request.params['phone']):
            return err(400, 'Телефон уже зарегистрирован')
        user_data = await validate_registration_new(
            email = request.params['email'],
            email_code = request.params['email_code'],
            phone_code = request.params['phone_code'],
        )
        if user_data:
            await user.create(
                name = request.params['name'],
                email = user_data['email'],
                phone = user_data['phone'],
                company = request.params['company'],
                position = request.params['position'],
                password = request.params['password'],
                roles = user_data['roles'],
                annual = request.params['annual'],
                annual_privacy = request.params['annual_privacy'],
                employees = request.params['employees'],
                employees_privacy = request.params['employees_privacy'],
                catalog = request.params['catalog'],
                tags = request.params['tags'],
                interests = request.params['interests'],
                city = request.params['city'],
                hobby = request.params['hobby'],
                birthdate = request.params['birthdate'],
                birthdate_privacy = request.params['birthdate_privacy'],
                experience = request.params['experience'],
            )
            await request.session.assign(user.id)
            request.user.copy(user = user)
            request.api.websocket_update(request.session.id, request.user.id)
            dispatch('user_register', request)
            return OrjsonResponse({})
        else:
            return err(403, 'Проверочный код не верен')
    else:
        return err(400, 'Не указан email')



################################################################
async def change_email(request):
    await asyncio.sleep(.25)
    if request.user.id:
        if validate(request.params, MODELS['change_email']):
            user_check = User()
            if not await user_check.find(email = request.params['account']):
                code = str(randint(1000, 9999))
                await request.user.set_change_code(code = code, type = 'EMAIL')
                #print(code)
                send_email(request.api.stream_email, request.params['account'], CHANGE_EMAIL_CODE['subject'], CHANGE_EMAIL_CODE['body'], { 'code': code })
                return OrjsonResponse({})
            else:
                return err(400, 'Email уже зарегистрирован')
        else:
            return err(400, 'Не указан email')
    else:
        return err(403, 'Нет доступа')



################################################################
async def change_email_validate(request):
    await asyncio.sleep(.5)
    if request.user.id:
        if validate(request.params, MODELS['change_email_validate']):
            user_check = User()
            if not await user_check.find(email = request.params['account']):
                if await request.user.check_change_code(code = request.params['code'], type = 'EMAIL'):
                    await request.user.update_email(request.params['account'])
                    return OrjsonResponse({})
                else:
                    return err(403, 'Код не верен')
            else:
                return err(400, 'Email уже зарегистрирован')
        else:
            return err(400, 'Не указан email')
    else:
        return err(403, 'Нет доступа')



################################################################
async def change_mobile(request):
    await asyncio.sleep(.25)
    if request.user.id:
        if validate(request.params, MODELS['change_mobile']):
            user_check = User()
            if not await user_check.find(phone = request.params['account']):
                code = str(randint(1000, 9999))
                await request.user.set_change_code(code = code, type = 'MOBILE')
                #print(code)
                send_mobile_message(request.api.stream_mobile, request.params['account'], CHANGE_MOBILE_CODE['message'], { 'code': code })
                return OrjsonResponse({})
            else:
                return err(400, 'Телефон уже зарегистрирован')
        else:
            return err(400, 'Не указан email')
    else:
        return err(403, 'Нет доступа')



################################################################
async def change_mobile_validate(request):
    await asyncio.sleep(.5)
    if request.user.id:
        if validate(request.params, MODELS['change_mobile_validate']):
            user_check = User()
            if not await user_check.find(email = request.params['account']):
                if await request.user.check_change_code(code = request.params['code'], type = 'MOBILE'):
                    await request.user.update_phone(request.params['account'])
                    return OrjsonResponse({})
                else:
                    return err(403, 'Код не верен')
            else:
                return err(400, 'Телефон уже зарегистрирован')
        else:
            return err(400, 'Не указан email')
    else:
        return err(403, 'Нет доступа')



################################################################
async def change_credentials(request):
    await asyncio.sleep(.5)
    if request.user.id:
        if validate(request.params, MODELS['change_credentials']):
            await request.user.update_password(request.params['code'])
            return OrjsonResponse({})
        else:
            return err(400, 'Неверные данные')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_login(request):
    await asyncio.sleep(.5)
    if validate(request.params, MODELS['moderator_login']):
        user = User()        
        if await user.check(request.params['account'], request.params['password']):
            if set(user.roles) & { 'admin', 'moderator', 'manager', 'community manager', 'chief', 'agent', 'organizer' }:
                await request.session.assign(user.id)
                request.user.copy(user = user)
                request.api.websocket_update(request.session.id, request.user.id)
                # dispatch('user_login', request)
                return OrjsonResponse({})
            else:
                return err(403, 'Нет доступа')
        else:
            return err(403, 'Пользователь и / или пароль не верны')
    else:
        return err(400, 'Не указаны логин и / или пароль')



################################################################
async def manager_login_email_validate(request):
    await asyncio.sleep(.5)
    if validate(request.params, MODELS['moderator_login_email_validate']):
        user = User()
        if await user.find(email = request.params['account']) and user.active:
            if await user.check_validation_code(code = request.params['code']):
                if set(user.roles) & { 'admin', 'moderator', 'manager', 'community manager', 'chief', 'agent', 'organizer' }:
                    await request.session.assign(user.id)
                    request.user.copy(user = user)
                    request.api.websocket_update(request.session.id, request.user.id)
                    return OrjsonResponse({})
                else:
                    return err(403, 'Нет доступа')
            else:
                return err(403, 'Код не верен')
        else:
            return err(404, 'Пользователь не найден')
    else:
        return err(400, 'Не указан email')



################################################################
async def manager_login_mobile_validate(request):
    await asyncio.sleep(.5)
    if validate(request.params, MODELS['moderator_login_mobile_validate']):
        user = User()
        if await user.find(phone = request.params['account']) and user.active:
            if await user.check_validation_code(code = request.params['code']):
                if set(user.roles) & { 'admin', 'moderator', 'manager', 'community manager', 'chief', 'agent', 'organizer' }:
                    await request.session.assign(user.id)
                    request.user.copy(user = user)
                    request.api.websocket_update(request.session.id, request.user.id)
                    return OrjsonResponse({})
                else:
                    return err(403, 'Нет доступа')
            else:
                return err(403, 'Код не верен')
        else:
            return err(404, 'Пользователь не найден')
    else:
        return err(400, 'Не указан телефон')



################################################################
async def register_device(request):
    if validate(request.params, MODELS['register_device']):
        device_info = None
        if request.params['device_info']:
            try:
                device_info = orjson.loads(request.params['device_info'])
            except JSONDecodeError as e:
                device_info = None
                print('Device info parsing error:', e)
        await request.session.register_device(request.params['device_id'], device_info, request.params['device_token'])
        return OrjsonResponse({})
    else:
        return err(400, 'Неверные данные')
