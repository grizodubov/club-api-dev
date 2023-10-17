from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.item import Item, Items
from app.models.notification import send_notification



def routes():
    return [
        Route('/item/{id:int}/view', item_view, methods = [ 'POST' ]),
        Route('/item/view', items_view, methods = [ 'POST' ]),
    ]



MODELS = {
	'item_view': {
		'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
	'items_view': {
		'ids': {
			'required': True,
			'type': 'int',
            'value_min': 1,
            'list': True,
		},
	},
}



################################################################
async def item_view(request):
    if request.user.id:
        if validate(request.path_params, MODELS['item_view']):
            item = Item()
            await item.set(id = request.path_params['id'])
            if item.id:
                time_view = await item.view(request.user.id)
                dispatch('item_view', request)
                send_notification(request.user.id)
                return OrjsonResponse({
                    'item_id': item.id,
                    'time_view': time_view,
                })
            else:
                return err(404, 'Объект не найдено')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def items_view(request):
    if request.user.id:
        if validate(request.params, MODELS['items_view']):
            items = Items()
            await items.set(ids = request.params['ids'])
            if items.list:
                time_view = await items.view(request.user.id)
                dispatch('item_view', request)
                send_notification(request.user.id)
                return OrjsonResponse({
                    'items_ids': items.ids(),
                    'time_view': time_view,
                })
            else:
                return err(404, 'Объект не найдено')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')
