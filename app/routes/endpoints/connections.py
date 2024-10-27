from starlette.routing import Route


from app.core.context import get_api_context
from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.connections import get_connections_list
from app.models.user import get_community_managers



def routes():
    return [
        Route('/ma/connections/control', manager_get_connections, methods = [ 'POST' ]),
    ]



MODELS = {
    'manager_get_connections': {
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
        'state': {
            'required': True,
            'type': 'bool',
            'null': True,
        },
        'form': {
            'required': True,
            'type': 'str',
            'values': [ 'all', 'event', 'contact' ],
        },
        'evaluation': {
            'required': True,
            'type': 'bool',
            'list': True,
        },
        'date_creation': {
            'required': True,
            'type': 'int',
            'null': True,
        },
	},
}



################################################################
async def manager_get_connections(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief' }):
        if validate(request.params, MODELS['manager_get_connections']):
            community_manager_id = request.params['community_manager_id']
            data = await get_connections_list(
                page = request.params['page'],
                community_manager_id = community_manager_id,
                state = request.params['state'],
                form = request.params['form'],
                evaluation = request.params['evaluation'],
                date_creation = request.params['date_creation'],
            )
            community_managers = await get_community_managers()
            return OrjsonResponse({
                'amount': data[0],
                'connections': data[1],
                'page': data[2],
                'community_managers': community_managers,
            })
        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')
