from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.tag import get_tags



def routes():
    return [
        Route('/m/tag/list', moderator_list_tags, methods = [ 'POST' ]),
    ]



MODELS = {
}



################################################################
async def moderator_list_tags(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager' }):
        tags = await get_tags()
        return OrjsonResponse({
            'tags': [ v.update({ 'tag': k }) for k, v in tags.items() ]
        })
    else:
        return err(403, 'Нет доступа')
