import orjson
from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.utils.validate import validate
from app.models.filter import save_filter, load_filter



def routes():
    return [
        Route('/ma/filter/save', manager_filter_save, methods = [ 'POST' ]),
        Route('/ma/filter/load', manager_filter_load, methods = [ 'POST' ]),
    ]



MODELS = {
    'manager_filter_save': {
        'type': {
            'required': True,
            'type': 'str',
            'values': [ 'events' ],
        },
        'name': {
            'required': True,
            'type': 'str',
            'length_min': 1,
        },
        'filter': {
            'required': True,
            'type': 'str',
            'length_min': 2,
        },
    },
    'manager_filter_load': {
        'type': {
            'required': True,
            'type': 'str',
        },
        'name': {
            'required': True,
            'type': 'str',
        },
    },
}



################################################################
async def manager_filter_save(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_filter_save']):
            await save_filter(request.user.id, request.params['type'], request.params['name'], request.params['filter'])
            return OrjsonResponse({
                'filter': request.params['filter'],
            })
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_filter_load(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_filter_load']):
            filter = await load_filter(request.user.id, request.params['type'], request.params['name'])
            return OrjsonResponse({
                'filter': filter,
            })
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')
