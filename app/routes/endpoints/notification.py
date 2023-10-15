from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.models.notification import get_notifications




def routes():
    return [
        Route('/notifications/list', notifications_list, methods = [ 'POST' ]),
        Route('/notifications/list/before/{ts:int}', notifications_list, methods = [ 'POST' ]),
    ]



MODELS = {
}



################################################################
async def notifications_list(request):
    if request.user.id:
        before = None
        if 'ts' in request.path_params:
            before = request.path_params['ts']
        result = await get_notifications(request.user.id, before)
        return OrjsonResponse({
            'notifications': result,
        })
    else:
        return err(403, 'Нет доступа')
