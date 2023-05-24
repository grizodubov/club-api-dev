from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.stats import get_tags_stats, get_users_stats



def routes():
    return [
        Route('/m/stats/tags', moderator_stats_tags, methods = [ 'POST' ]),
        Route('/m/stats/users', moderator_stats_users, methods = [ 'POST' ]),
    ]



MODELS = {
}



################################################################
async def moderator_stats_tags(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator' }):
        return OrjsonResponse(await get_tags_stats())
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_stats_users(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator' }):
        return OrjsonResponse(await get_users_stats())
    else:
        return err(403, 'Нет доступа')
