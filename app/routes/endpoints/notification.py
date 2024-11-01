from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.models.notification import get_notifications, get_highlights, notifications_read_all




def routes():
    return [
        Route('/notifications/list', notifications_list, methods = [ 'POST' ]),
        Route('/notifications/list/before/{ts:int}', notifications_list, methods = [ 'POST' ]),
        Route('/notifications/read/all', notifications_all, methods = [ 'POST' ]),
    ]



MODELS = {
}



################################################################
async def notifications_list(request):
    if request.user.id:
        before = None
        if 'ts' in request.path_params:
            before = request.path_params['ts']
        notifications = await get_notifications(request.user.id, before)
        highlights = await get_highlights(request.user.id)
        return OrjsonResponse({
            'notifications': notifications,
            'highlights': highlights,
        })
    else:
        return err(403, 'Нет доступа')
    


################################################################
async def notifications_all(request):
    if request.user.id:
        await notifications_read_all(request.user.id)
        return OrjsonResponse({})
    else:
        return err(403, 'Нет доступа')
