from starlette.routing import Route


from app.core.context import get_api_context
from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.log import get_sign_log, get_views
from app.models.user import get_community_managers



def routes():
    return [
        Route('/m/log/signings', moderator_get_signings, methods = [ 'POST' ]),
        Route('/ma/log/signings', manager_get_signings, methods = [ 'POST' ]),
        Route('/ma/log/views', manager_get_views, methods = [ 'POST' ]),
    ]



MODELS = {
	'moderator_get_signings': {
        'page': {
            'required': True,
            'type': 'int',
            'value_min': 1,
            'default': 1,
        },
        'roles': {
            'required': True,
			'type': 'str',
            'list': True,
            'null': True,
            'values': [ 'client', 'community manager' ],
        },
	},
	'manager_get_signings': {
        'page': {
            'required': True,
            'type': 'int',
            'value_min': 1,
            'default': 1,
        },
        'community_manager_id': {
            'required': True,
            'type': 'int',
            'value_min': 0,
            'null': True,
        },
	},
    'manager_get_views': {
        'page': {
            'required': True,
            'type': 'int',
            'value_min': 1,
            'default': 1,
        },
        'community_manager_id': {
            'required': True,
            'type': 'int',
            'value_min': 0,
            'null': True,
        },
	},
}



################################################################
async def moderator_get_signings(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager' }):
        if validate(request.params, MODELS['moderator_get_signings']):
            log_data = await get_sign_log(
                page = request.params['page'],
                roles = request.params['roles'],
            )
            return OrjsonResponse({
                'amount': log_data[0],
                'log': log_data[1],
            })
        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_get_signings(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_get_signings']):
            community_manager_id = request.params['community_manager_id']
            if not request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief' }):
                community_manager_id = request.user.id
            log_data = await get_sign_log(
                page = request.params['page'],
                roles = [ 'client' ],
                community_manager_id = community_manager_id,
            )
            community_managers = await get_community_managers()
            return OrjsonResponse({
                'amount': log_data[0],
                'log': log_data[1],
                'community_managers': community_managers,
            })
        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_get_views(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief' }):
        if validate(request.params, MODELS['manager_get_views']):
            community_manager_id = request.params['community_manager_id']
            views_data = await get_views(
                page = request.params['page'],
                community_manager_id = community_manager_id,
            )
            community_managers = await get_community_managers()
            return OrjsonResponse({
                'amount': views_data[0],
                'log': views_data[1],
                'community_managers': community_managers,
            })
        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')
