from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.notification_1 import get_list, get_stats, view



def routes():
    return [
        Route('/notification/1/list', notifications_list, methods = [ 'POST' ]),
        Route('/notification/1/view', notification_view, methods = [ 'POST' ]),
        Route('/notification/1/view/all', notification_view_all, methods = [ 'POST' ]),
    ]



MODELS = {
    'notifications_list': {
        'time_breakpoint': {
            'required': True,
			'type': 'str',
            'null': True,
        },
        'limit': {
            'required': True,
			'type': 'int',
            'null': True,
        },
    },
    'notification_view': {
        'time_notify': {
            'required': True,
			'type': 'str',
        },
    },
}



################################################################
async def notifications_list(request):
    if request.user.id:
        if validate(request.params, MODELS['notifications_list']):
            notifications = await get_list(
                user_id = request.user.id,
                time_breakpoint = request.params['time_breakpoint'],
                limit = request.params['limit'],
            )
            stats = await get_stats(
                user_id = request.user.id
            )
            return OrjsonResponse({
                'notifications': notifications,
                'stats': stats,
            })
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def notification_view(request):
    if request.user.id:
        if validate(request.params, MODELS['notification_view']):
            await view(
                user_id = request.user.id,
                time_notify = request.params['time_notify'],
            )
            return OrjsonResponse({})
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def notification_view_all(request):
    if request.user.id:
        await view(
            user_id = request.user.id,
            time_notify = None,
        )
        return OrjsonResponse({})
    else:
        return err(403, 'Нет доступа')
