from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.utils.validate import validate
from app.models.avatar import Avatar
from app.models.item import Item



def routes():
    return [
        Route('/avatar/create', create_avatar, methods = [ 'POST' ]),
    ]



MODELS = {
	'create_avatar': {
		'owner_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
}



################################################################
async def create_avatar(request):
    if validate(request.params, MODELS['create_avatar']):
        if request.user.id == 1010:
            item = Item()
            await item.set(id = request.params['owner_id'])
            if item.id and item.model in { 'user', 'group', 'community' }:
                avatar = Avatar()
                await avatar.create(
                    owner_id = request.params['owner_id'],
                    owner_model = item.model
                )
                if avatar.id:
                    return OrjsonResponse({
                        'hash': avatar.hash,
                    })
                else:
                    return err(404, 'Аватар не может быть загружен')
            else:
                return err(404, 'Аватар не может быть загружен')
        else:
            return err(403, 'Нет доступа')
    else:
        return err(400, 'Неверный токен')
