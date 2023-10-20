from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.tag import get_tags, update_tag



def routes():
    return [
        Route('/m/tag/list', moderator_list_tags, methods = [ 'POST' ]),
        Route('/m/tag/replace', moderator_replace_tag, methods = [ 'POST' ]),
        Route('/m/tag/delete', moderator_delete_tag, methods = [ 'POST' ]),
    ]



MODELS = {
    'moderator_replace_tag': {
		'tag': {
			'required': True,
			'type': 'str',
		},
		'tag_new': {
			'required': True,
			'type': 'str',
		},
	},
    'moderator_delete_tag': {
		'tag': {
			'required': True,
			'type': 'str',
		},
	},
}



################################################################
async def moderator_list_tags(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        tags = await get_tags()
        return OrjsonResponse({
            'tags': [
                {
                    'tag': k,
                    'options': list(v['options']),
                    'competency': v['competency'],
                    'interests': v['interests'],
                    'communities': v['communities'],
                } for k, v in tags.items()
            ]
        })
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_replace_tag(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        if validate(request.params, MODELS['moderator_replace_tag']):
            await update_tag(request.params['tag'], request.params['tag_new'])
            dispatch('tag_update', request)
            return OrjsonResponse({})
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_delete_tag(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        if validate(request.params, MODELS['moderator_delete_tag']):
            await update_tag(request.params['tag'], '')
            dispatch('tag_update', request)
            return OrjsonResponse({})
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')
