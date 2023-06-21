import asyncio
from random import randint
from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.user import User, validate_registration
from app.models.session import check_by_token
from app.helpers.email import send_email
from app.helpers.mobile import send_mobile_message
from app.helpers.templates import VERIFICATION_CODE



def routes():
    return [
        Route('/login/mobile', login_mobile, methods = [ 'POST' ]),
        Route('/login/mobile/validate', login_mobile_validate, methods = [ 'POST' ]),
        Route('/login/email', login_email, methods = [ 'POST' ]),
        Route('/login/email/validate', login_email_validate, methods = [ 'POST' ]),
        Route('/login', login, methods = [ 'POST' ]),
        Route('/logout', logout, methods = [ 'POST' ]),
        Route('/check/token', check_token, methods = [ 'POST' ]),
        Route('/register', register, methods = [ 'POST' ]),
        Route('/register/validate', register_validate, methods = [ 'POST' ]),
        Route('/terminate', terminate, methods = [ 'POST' ]),

        Route('/m/login', moderator_login, methods = [ 'POST' ]),
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
}



################################################################
async def login(request):
    await asyncio.sleep(.5)
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
        if await user.find(email = request.params['account']):
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
        if await user.find(email = request.params['account']):
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
        if await user.find(phone = request.params['account']):
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
        if await user.find(phone = request.params['account']):
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
            if set(user.roles) & { 'admin', 'moderator', 'editor' }:
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
