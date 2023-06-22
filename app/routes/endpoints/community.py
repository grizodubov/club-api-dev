from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.community import Community
from app.models.user import User



def routes():
    return [
        Route('/m/community/search', moderator_community_search, methods = [ 'POST' ]),
        Route('/m/community/update', moderator_community_update, methods = [ 'POST' ]),
        Route('/m/community/create', moderator_community_create, methods = [ 'POST' ]),
    ]



MODELS = {
    # moderator
	'moderator_community_search': {
		'text': {
			'required': True,
			'type': 'str',
		},
        'page': {
            'required': True,
            'type': 'int',
            'value_min': 1,
            'default': 1,
        },
	},
	'moderator_community_update': {
		'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
		'name': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'description': {
			'required': True,
			'type': 'str',
		},
		'members': {
			'required': True,
			'type': 'int',
            'list': True,
            'null': True,
		},
	},
	'moderator_community_create': {
		'name': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'description': {
			'required': True,
			'type': 'str',
		},
	},
}



################################################################
async def moderator_community_search(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator' }):
        if validate(request.params, MODELS['moderator_community_search']):
            (result, amount) = await Community.search(
                text = request.params['text'],
                offset = (request.params['page'] - 1) * 10,
                limit = 10,
                count = True,
            )
            users = await User.hash()
            return OrjsonResponse({
                'communities': [ item.dump() for item in result ],
                'users': users,
                'amount': amount,
            })
        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_community_update(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator' }):
        if validate(request.params, MODELS['moderator_community_update']):
            community = Community()
            await community.set(id = request.params['id'])
            if community.id:
                await community.update(**request.params)
                dispatch('community_update', request)
                return OrjsonResponse({})
            else:
                return err(404, 'Группа не найдена')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_community_create(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator' }):
        if validate(request.params, MODELS['moderator_community_create']):
            community = Community()
            await community.create(
                name = request.params['name'],
                description = request.params['description'],
            )
            dispatch('community_create', request)
            return OrjsonResponse({})
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')
