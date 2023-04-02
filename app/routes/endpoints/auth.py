import asyncio
from random import randint
from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.context import get_api_context
from app.utils.validate import validate
from app.models.user import User
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
	'login_phone': {
		'account': {
			'required': True,
			'type': 'str',
            'length_min': 4,
            'processing': lambda x: x.strip().lower(),
		},
	},
	'login_phone_validate': {
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
}



################################################################
async def login(request):
    await asyncio.sleep(.5)
    if validate(request.params, MODELS['login']):
        user = User()        
        if await user.check(request.params['account'], request.params['password']):
            await request.session.assign(user.id)
            await request.user.copy(user = user)
            request.api.websocket_update(request.session.id, request.user.id)
            return OrjsonResponse({})
        else:
            return err(403, 'Пользователь и / или пароль не верны')
    else:
        return err(400, 'Не указаны логин и / или пароль')



################################################################
async def logout(request):
    await request.session.assign(0)
    request.user.reset()
    request.api.websocket_update(request.session.id, request.user.id)
    return OrjsonResponse({})



################################################################
async def login_email(request):
    await asyncio.sleep(.5)
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
    pass



################################################################
async def login_mobile(request):
    await asyncio.sleep(.5)
    if validate(request.params, MODELS['login_phone']):
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
    pass
