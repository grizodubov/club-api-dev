from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.stats import get_tags_stats, get_users_stats, get_new_clients_stats, get_signings_stats, get_unique_views_stats



def routes():
    return [
        Route('/m/stats/tags', moderator_stats_tags, methods = [ 'POST' ]),
        Route('/m/stats/users', moderator_stats_users, methods = [ 'POST' ]),

        Route('/ma/stats/clients/new', manager_get_new_clients, methods = [ 'POST' ]),
        Route('/ma/stats/signings', manager_get_signings, methods = [ 'POST' ]),
        Route('/ma/stats/views/unique', manager_get_unique_views, methods = [ 'POST' ]),
    ]



MODELS = {
    'manager_get_new_clients': {
        'date': {
			'required': True,
			'type': 'str',
        },
    },
    'manager_get_signings': {
        'date': {
			'required': True,
			'type': 'str',
        },
    },
    'manager_get_unique_views': {
        'date': {
			'required': True,
			'type': 'str',
        },
    },
}



################################################################
async def moderator_stats_tags(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        return OrjsonResponse(await get_tags_stats())
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_stats_users(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        return OrjsonResponse(await get_users_stats())
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_get_new_clients(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_get_new_clients']):
            return OrjsonResponse(await get_new_clients_stats(request.params['date']))
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_get_signings(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_get_signings']):
            return OrjsonResponse(await get_signings_stats(request.params['date']))
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_get_unique_views(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_get_unique_views']):
            return OrjsonResponse(await get_unique_views_stats(request.params['date']))
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')
