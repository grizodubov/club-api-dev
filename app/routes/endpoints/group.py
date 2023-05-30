from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.group import Group
from app.models.user import User



def routes():
    return [
        Route('/m/group/search', moderator_group_search, methods = [ 'POST' ]),
        Route('/m/group/update', moderator_group_update, methods = [ 'POST' ]),
        Route('/m/group/create', moderator_group_create, methods = [ 'POST' ]),
    ]



MODELS = {
    # moderator
	'moderator_group_search': {
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
	'moderator_group_update': {
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
		'users': {
			'required': True,
			'type': 'int',
            'list': True,
            'null': True,
		},
	},
	'moderator_group_create': {
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
async def moderator_group_search(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator' }):
        if validate(request.params, MODELS['moderator_group_search']):
            (result, amount) = await Group.search(
                text = request.params['text'],
                offset = (request.params['page'] - 1) * 10,
                limit = 10,
                count = True,
            )
            users = await User.hash()
            return OrjsonResponse({
                'groups': [ item.dump() for item in result ],
                'users': users,
                'amount': amount,
            })
        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_group_update(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator' }):
        if validate(request.params, MODELS['moderator_group_update']):
            group = Group()
            await group.set(id = request.params['id'])
            if group.id:
                await group.update(**request.params)
                dispatch('group_update', request)
                return OrjsonResponse({})
            else:
                return err(404, 'Группа не найдена')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_group_create(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator' }):
        if validate(request.params, MODELS['moderator_group_create']):
            group = Group()
            await group.create(
                name = request.params['name'],
                description = request.params['description'],
            )
            dispatch('group_create', request)
            return OrjsonResponse({})
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')
