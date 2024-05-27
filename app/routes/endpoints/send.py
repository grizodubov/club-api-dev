from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.send import send_message



def routes():
    return [
        Route('/m/send/notification', moderator_send_notification, methods = [ 'POST' ]),
    ]



MODELS = {
	'moderator_send_notification': {
		'flag_email': {
			'required': True,
			'type': 'bool',
		},
		'flag_sms': {
			'required': True,
			'type': 'bool',
		},
		'flag_push': {
			'required': True,
			'type': 'bool',
		},
        'title': {
            'required': True,
			'type': 'str',
            'length_min': 1,
        },
        'message': {
            'required': True,
			'type': 'str',
            'length_min': 1,
        },
        'link': {
            'required': True,
			'type': 'str',
        },
	},
}



################################################################
async def moderator_send_notification(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator' }):
        if validate(request.params, MODELS['moderator_send_notification']):
            await send_message(request.user.id, **request.params)
            return OrjsonResponse({})
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')
