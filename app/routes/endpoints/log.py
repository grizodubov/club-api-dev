from starlette.routing import Route


from app.core.context import get_api_context
from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.log import get_sign_log



def routes():
    return [
        Route('/m/log/signings', moderator_get_signings, methods = [ 'POST' ]),
    ]



MODELS = {
	'moderator_get_signings': {
        'page': {
            'required': True,
            'type': 'int',
            'value_min': 1,
            'default': 1,
        },
	},
}



################################################################
async def moderator_get_signings(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager' }):
        if validate(request.params, MODELS['moderator_get_signings']):
            log_data = await get_sign_log(page = request.params['page'])
            return OrjsonResponse({
                'amount': log_data[0],
                'log': log_data[1],
            })
        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')
