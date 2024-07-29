import orjson
from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.utils.validate import validate
from app.models.report import get_clients, create_clients_file
from app.models.event import get_events_for_report
from app.models.user import get_community_managers_for_report



def routes():
    return [
        Route('/ma/report/clients/create', manager_report_clients_create, methods = [ 'POST' ]),
        Route('/ma/report/events/list', manager_report_events_list, methods = [ 'POST' ]),
        Route('/ma/report/managers/list', manager_report_managers_list, methods = [ 'POST' ]),
    ]



MODELS = {
    # manager
	'manager_report_clients_create': {
        'config': {
            'required': True,
            'type': 'str',
        },
	},
}



################################################################
async def manager_report_clients_create(request):
    if request.user.id and request.user.check_roles({ 'admin', 'editor', 'manager', 'chief', 'community manager', 'agent', 'curator' }):
        if validate(request.params, MODELS['manager_report_clients_create']):
            config = None
            try:
                config = orjson.loads(request.params['config'])
            except orjson.JSONDecodeError as e:
                return err(400, 'Неверный запрос')
            if config is not None:
                clients_ids = await request.user.get_allowed_clients_ids()
                result = await get_clients(config, clients_ids)
                file = create_clients_file(result)
                return OrjsonResponse({
                    'clients': result,
                    'file': file,
                })
            else:
                return err(400, 'Неверный запрос')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_report_events_list(request):
    if request.user.id and request.user.check_roles({ 'admin', 'editor', 'manager', 'chief', 'community manager', 'agent', 'curator' }):
        events = await get_events_for_report()
        return OrjsonResponse({
            'events': events,
        })
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_report_managers_list(request):
    if request.user.id and request.user.check_roles({ 'admin', 'editor', 'manager', 'chief', 'community manager', 'agent', 'curator' }):
        agent_id = None
        community_manager_id = None
        if request.user.check_roles({ 'agent' }) and not request.user.check_roles({ 'admin', 'editor', 'manager', 'chief', 'curator' }):
            agent_id = request.user.id
        if request.user.check_roles({ 'community manager' }) and not request.user.check_roles({ 'admin', 'editor', 'manager', 'chief', 'curator' }):
            community_manager_id = request.user.id
        managers = await get_community_managers_for_report(agent_id = agent_id, community_manager_id = community_manager_id)
        return OrjsonResponse({
            'managers': managers,
        })
    else:
        return err(403, 'Нет доступа')
