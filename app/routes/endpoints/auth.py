import re
import asyncio
import pytz
import datetime
from random import randint
from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.context import get_api_context
from app.utils.validate import validate
from app.utils.query import FilterGroup
from app.models import user, card, firm
from app.core.mail import send_email
from app.models.mail import send_password
from app.models.trade import send_mobile_message
from app.models.event import dispatch
from app.models.user import phone_login, phone_login_validate



def routes():
    return [
        Route('/login', login, methods = [ 'POST' ]),
        Route('/login/phone/check', login_phone, methods = [ 'POST' ]),
        Route('/login/phone/validate', login_phone_validate, methods = [ 'POST' ]),
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
		'phone_code': {
			'required': True,
			'type': 'str',
            'length': 4,
            'pattern': r'^\d{4}$',
		},
	},
	'regcheck': {
		'email': {
			'required': True,
			'type': 'str',
            'length_min': 4,
            'pattern': r'^\s*[^\@\s]+\@[^\@\s]{4,}\s*$',
            'processing': lambda x: x.strip().lower(),
		},
		'phone': {
			'required': True,
			'type': 'str',
            'length_min': 10,
            'pattern': r'^[\d\s\(\)\+\-]+$',
            'processing': lambda x: x.strip(),
		},
        'source': {
			'type': 'str',
            'null': True,
            'default': None,
		},
	},
	'regvalidate': {
		'email': {
			'required': True,
			'type': 'str',
            'length_min': 4,
            'pattern': r'^\s*[^\@\s]+\@[^\@\s]{4,}\s*$',
            'processing': lambda x: x.strip().lower(),
		},
		'phone': {
			'required': True,
			'type': 'str',
            'length_min': 10,
            'pattern': r'^[\d\s\(\)\+\-]+$',
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
	},
	'regfinal': {
		'name': {
			'required': True,
			'type': 'str',
            'length_min': 4,
		},
		'email': {
			'required': True,
			'type': 'str',
            'length_min': 4,
            'pattern': r'^\s*[^\@\s]+\@[^\@\s]{4,}\s*$',
            'processing': lambda x: x.strip().lower(),
		},
		'phone': {
			'required': True,
			'type': 'str',
            'length_min': 10,
            'pattern': r'^[\d\s\(\)\+\-]+$',
            'processing': lambda x: x.strip(),
		},
		'password': {
			'required': True,
			'type': 'str',
            'length_min': 4,
		},
		'position': {
			'required': True,
			'type': 'str',
            'length_min': 2,
		},
		'firm_name': {
			'required': True,
			'type': 'str',
            'length_min': 2,
		},
		'firm_inn': {
			'required': True,
			'type': 'str',
            'length_min': 2,
		},   	
    },
	'recover': {
		'account': {
			'required': True,
			'type': 'str',
            'length_min': 4,
            'pattern': r'^\s*[^\@\s]+\@[^\@\s]{4,}\s*$',
            'processing': lambda x: x.strip().lower(),
		},
    },
}



################################################################
async def login(request):
    await asyncio.sleep(.5)
    if validate(request.params, MODELS['login']):
        phone_processed = '+7' + ''.join(list(re.sub(r'[^\d]+', '', request.params['account']))[-10:])
        user_attrs = await user.find(
            FilterGroup(
                user.FIELDS,
                None,
                {
                    'login': request.params['account'],
                    'email': request.params['account'],
                    'phone': phone_processed,
                },
                'OR'
            ),
            row = 1,
        )
        if user_attrs and user_attrs['id']:
            if user_attrs['card_id']:
                c = card.Card()
                await c.set(id = user_attrs['card_id'])
                if c.id:
                    if c.inactive:
                        return err(403, 'Нет доступа')
                    sd = round(datetime.datetime.now(tz = pytz.utc).replace(tzinfo = None).timestamp() * 1000) - 86400000
                    if c.stop_date and sd > c.stop_date:
                        return err(403, 'Нет доступа')
                else:
                    return err(404, 'Пользователь не найден')
            if request.params['password'] == user_attrs['_password']:
                await request.session.assign(user_attrs['id'])
                await request.user.set(attrs = user_attrs)
                if request.user.card_id:
                    request.user.card = c
                request.api.websocket_update(request.session.id, request.user.id)
                request.annex.update({ '_filters': await request.session.filters_update(request.user, request.filters) })
                # event
                return OrjsonResponse({})
            else:
                return err(403, 'Пароль не верен')
        else:
            return err(404, 'Пользователь не найден')
    else:
        return err(400, 'Не указаны логин и / или пароль')



################################################################
async def logout(request):
    await request.session.assign(0)
    request.user.reset()
    request.api.websocket_update(request.session.id, request.user.id)
    # event
    return OrjsonResponse({})



################################################################
async def login_phone(request):
    await asyncio.sleep(.5)
    if validate(request.params, MODELS['login_phone']):
        phone_processed = '+7' + ''.join(list(re.sub(r'[^\d]+', '', request.params['account']))[-10:])
        user_attrs = await user.find(
            FilterGroup(
                user.FIELDS,
                None,
                {
                    'login': request.params['account'],
                    'email': request.params['account'],
                    'phone': phone_processed,
                },
                'OR'
            ),
            row = 1,
        )
        if user_attrs and user_attrs['id']:
            if user_attrs['phone']:
                phone_code = str(randint(1000, 9999))
                phone_txt = user_attrs['phone'][0:2] + ' (' + user_attrs['phone'][2:5] + ') ' + user_attrs['phone'][5:]
                await phone_login(user_attrs['id'], phone_code)
                await send_mobile_message(re.sub(r'[^\d\+]+', '', phone_txt), 'Код подтверждения входа: ' + phone_code)
            return OrjsonResponse({})
        else:
            return err(404, 'Пользователь не найден')
    else:
        return err(400, 'Не указаны логин и / или пароль')



################################################################
async def login_phone_validate(request):
    await asyncio.sleep(.5)
    if validate(request.params, MODELS['login_phone_validate']):
        phone_processed = '+7' + ''.join(list(re.sub(r'[^\d]+', '', request.params['account']))[-10:])
        user_attrs = await user.find(
            FilterGroup(
                user.FIELDS,
                None,
                {
                    'login': request.params['account'],
                    'email': request.params['account'],
                    'phone': phone_processed,
                },
                'OR'
            ),
            row = 1,
        )
        if user_attrs and user_attrs['id']:
            if await phone_login_validate(user_attrs['id'], request.params['phone_code']):
                await request.session.assign(user_attrs['id'])
                await request.user.set(attrs = user_attrs)
                if request.user.card_id:
                    request.user.card = card.Card()
                    await request.user.card.set(id = request.state.user.card_id)
                request.api.websocket_update(request.session.id, request.user.id)
                request.annex.update({ '_filters': await request.session.filters_update(request.user, request.filters) })
                # event
                return OrjsonResponse({})
            else:
                return err(403, 'Код не верен')
        else:
            return err(404, 'Пользователь не найден')
    else:
        return err(400, 'Неверный запрос')



################################################################
async def regcheck(request):
    no_check = {
        'email': [ 'forum0622' ],
        'phone': [],
    }
    if validate(request.params, MODELS['regcheck']):
        await asyncio.sleep(.2)
        phone = list(re.sub(r'[^\d]+', '', request.params['phone']))[-10:]
        phone_txt = ''
        for c in '+7 (xxx) xxxxxxx':
            if c == 'x':
                phone_txt += phone.pop(0)
            else:
                phone_txt += c
        check_user = await user.find(
            FilterGroup(
                user.FIELDS,
                None,
                {
                    'login': request.params['email'].lower(),
                    'email': request.params['email'].lower(),
                    'phone': phone_txt,
                },
                'OR'
            ),
            row = 1,
        )
        if not check_user:
            email_code = str(randint(1000, 9999)) if request.params['source'] is None or request.params['source'] not in no_check['email'] else '0000'
            await asyncio.sleep(.01)
            phone_code = str(randint(1000, 9999)) if request.params['source'] is None or request.params['source'] not in no_check['phone'] else '0000'
            await user.add_registration(request.params['email'], phone_txt, email_code, phone_code, request.params['source'])
            if email_code != '0000':
                send_email(
                    request.api.stream_mail,
                    request.params['email'],
                    'Подтверждение регистрации в системе Digitender',
                    """<p>Код подтверждения почтового адреса: <strong>""" + email_code + """</strong></p>"""
                )
            if phone_code != '0000':
                await send_mobile_message(re.sub(r'[^\d\+]+', '', phone_txt), 'Код подтверждения телефона: ' + phone_code)
            return OrjsonResponse({})
        else:
            return err(400, 'Пользователь уже зарегистрирован')
    else:
        return err(400, 'Неверный запрос')



################################################################
async def regvalidate(request):
    if validate(request.params, MODELS['regvalidate']):
        await asyncio.sleep(.2)
        phone = list(re.sub(r'[^\d]+', '', request.params['phone']))[-10:]
        phone_txt = ''
        for c in '+7 (xxx) xxxxxxx':
            if c == 'x':
                phone_txt += phone.pop(0)
            else:
                phone_txt += c
        check_user = await user.find(
            FilterGroup(
                user.FIELDS,
                None,
                {
                    'login': request.params['email'].lower(),
                    'email': request.params['email'].lower(),
                    'phone': phone_txt,
                },
                'OR'
            ),
            row = 1,
        )
        if not check_user:
            valid = await user.validate_registration(request.params['email'], phone_txt, request.params['email_code'], request.params['phone_code'])
            if valid:
                return OrjsonResponse({})
            else:
                return err(403, 'Ошибка в подтверждении')
        else:
            return err(400, 'Пользователь уже зарегистрирован')
    else:
        return err(400, 'Неверный запрос')



################################################################
async def regfinal(request):
    if validate(request.params, MODELS['regfinal']):
        await asyncio.sleep(.2)
        phone = list(re.sub(r'[^\d]+', '', request.params['phone']))[-10:]
        phone_txt = ''
        for c in '+7 (xxx) xxxxxxx':
            if c == 'x':
                phone_txt += phone.pop(0)
            else:
                phone_txt += c
        check_user = await user.find(
            FilterGroup(
                user.FIELDS,
                None,
                {
                    'login': request.params['email'].lower(),
                    'email': request.params['email'].lower(),
                    'phone': phone_txt,
                },
                'OR'
            ),
            row = 1,
        )
        if not check_user:
            c = card.Card()
            await c.create({
                'name': request.params['firm_name'],
                'manager_id': 10000
            })
            f = firm.Firm()
            await f.create({
                'name': request.params['firm_name'],
                'inn': request.params['firm_inn'],
                'card_id': c.id,
            })
            admin = user.User()
            await admin.create({
                'name': request.params['name'],
                'email': request.params['email'],
                'position': request.params['position'],
                'phone': phone_txt,
                'password': request.params['password'],
                'card_id': c.id,
                'role_id': [ 5 ],
            })
            await request.session.assign(admin.id)
            await request.user.set(id = admin.id)
            detail = {
                'item_data': {
                    'card_id': с.id,
                    'card_name': с.name,
                },
            }
            await dispatch(
                'user_create',
                request.user,
                {
                    'id': admin.id,
                    'spectator_id': c.id,
                } | detail,
                c.id
            )
            await dispatch(
                'card_create',
                request.user,
                {
                    'id': c.id,
                    'spectator_id': c.id,
                } | detail,
                c.id
            )
            await dispatch(
                'firm_create',
                request.user,
                {
                    'id': f.id,
                    'spectator_id': c.id,
                } | detail,
                c.id
            )
            send_password(
                request.api.stream_mail,
                {
                    'id': admin.id,
                    'name': admin.name,
                    'email': admin.email,
                    'password': admin._password,
                    '_password': admin._password,
                    'phone': admin.phone,
                }
            )
            await dispatch(
                'user_pass_send',
                request.user,
                {
                    'id': admin.id,
                    'spectator_id': admin.id,
                } | detail,
                admin.card_id
            )
            request.api.websocket_update(request.session.id, request.user.id)
            request.annex.update({ '_filters': await request.session.filters_update(request.user, request.filters) })
            return OrjsonResponse({})
        else:
            return err(400, 'Пользователь уже зарегистрирован')
    else:
        return err(400, 'Неверный запрос')



################################################################
async def recover(request):
    await asyncio.sleep(.5)
    if validate(request.params, MODELS['recover']):
        user_attrs = await user.find(
            FilterGroup(
                user.FIELDS,
                None,
                {
                    'email': request.params['account'],
                },
            ),
            row = 1,
        )
        if user_attrs and user_attrs['id']:
            send_password(
                request.api.stream_mail,
                {
                    'name': user_attrs['name'],
                    'password': user_attrs['_password'],
                    'email': user_attrs['email'],
                }
            )
            return OrjsonResponse({
                '_alert': 'Пароль отправлен на почту'
            })
        else:
            return err(404, 'Пользователь не найден')
    else:
        return err(400, 'Не указан почтовый адрес')
