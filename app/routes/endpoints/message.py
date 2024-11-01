from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.item import Item, Items
from app.models.user import User
from app.models.message import get_chats, get_messages, add_message, view_message, view_messages



def routes():
    return [
        Route('/message/list', messages_list, methods = [ 'POST' ]),
        Route('/message/add', message_add, methods = [ 'POST' ]),
        Route('/message/{id:int}/view', message_view, methods = [ 'POST' ]),
        Route('/message/view', messages_view, methods = [ 'POST' ]),
    ]



MODELS = {
	'messages_list': {
		'future_chat_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
            'null': True,
		},
		'chat_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
            'null': True,
		},
        'vector_type': {
            'required': True,
            'type': 'str',
            'values': [ 'init', 'reverse' ],
            'null': True,
        },
        'vector_id': {
            'required': True,
            'type': 'int',
            'value_min': 1,
            'null': True,
        },
	},
	'message_add': {
		'chat_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'text': {
            'required': True,
			'type': 'str',
            'length_min': 1,
        },
        'reply_to_message_id': {
			'required': True,
			'type': 'int',
            'null': True,
        },
	},
	'message_view': {
		'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
	'messages_view': {
		'ids': {
			'required': True,
			'type': 'int',
            'value_min': 1,
            'list': True,
		},
	},
}



################################################################
async def messages_list(request):
    if request.user.id:
        if validate(request.params, MODELS['messages_list']):
            chats = await get_chats(request.user.id, request.params['future_chat_id'], request.user.community_manager_id if request.user.community_manager_id else 1050)
            vector_type = None
            min_unread_message_id = None
            messages = []
            if request.params['chat_id']:
                item = Item()
                await item.set(id = request.params['chat_id'])
                if item.id:
                    access = True
                    if item.model == 'group':
                        access = await request.user.group_access(group_id = item.id)
                    if access:
                        vector_type = request.params['vector_type']
                        vector = None
                        if vector_type != 'init':
                            vector = {
                                'reverse': vector_type == 'reverse',
                                'id': request.params['vector_id'],
                            }
                        messages = await get_messages(
                            user_id = request.user.id,
                            chat_id = item.id,
                            chat_model = item.model,
                            init = vector_type == 'init',
                            vector = vector,
                        )
                        messages = sorted(messages, key = lambda m: m['id'])
                        if vector_type == 'init':
                            for m in reversed(messages):
                                if m['time_view']:
                                    break
                                min_unread_message_id = m['id']
                    else:
                        return err(403, 'Нет доступа')
                else:
                    return err(404, 'Сообщения не найдены')
            return OrjsonResponse({
                'vector_type': vector_type,
                'chats': chats,
                'chat_id': request.params['chat_id'],
                'min_unread_message_id': min_unread_message_id,
                'messages': messages,
            })
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def message_add(request):
    if request.user.id:
        if validate(request.params, MODELS['message_add']):
            item = Item()
            await item.set(id = request.params['chat_id'])
            print(item.__dict__)
            if item.id:
                access = True
                if item.model == 'group':
                    access = await request.user.group_access(group_id = item.id)
                if item.model == 'user':
                    if item.id == request.user.id:
                        return err(400, 'Неверный запрос')
                    user = User()
                    await user.set(id = item.id)
                    if user.id:
                        access = await request.user.check_access(user)
                    else:
                        access = False
                if access:
                    message = await add_message(
                        user_id = request.user.id,
                        chat_id = item.id,
                        chat_model = item.model,
                        text = request.params['text'],
                        reply_to_message_id = request.params['reply_to_message_id'] if request.params['reply_to_message_id'] else None
                    )
                    message['author_name'] = request.user.name
                    dispatch('message_add', request)
                    return OrjsonResponse({
                        'message': message
                    })
                else:
                    return err(403, 'Нет доступа')
            else:
                return err(404, 'Сообщения не найдены')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def message_view(request):
    if request.user.id:
        if validate(request.path_params, MODELS['message_view']):
            item = Item()
            await item.set(id = request.path_params['id'])
            if item.id and item.model == 'message':
                time_view = await view_message(request.user.id, item.id)
                dispatch('message_view', request)
                return OrjsonResponse({
                    'message_id': item.id,
                    'time_view': time_view,
                })
            else:
                return err(404, 'Сообщение не найдено')
        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')



################################################################
async def messages_view(request):
    if request.user.id:
        if validate(request.params, MODELS['messages_view']):
            items = Items()
            await items.set(ids = request.params['ids'])
            # TODO: проверить доступ к сообщениям
            if items.list and items.check_model('message'):
                if len(items.list) > 1:
                    data = await view_messages(request.user.id, items.ids())
                else:
                    time_view = await view_message(request.user.id, items.list[0].id)
                    data = [
                        {
                            'message_id': items.list[0].id,
                            'time_view': time_view,
                        },
                    ]
                dispatch('message_view', request)
                return OrjsonResponse({
                    'views': data,
                })
            else:
                return err(404, 'Сообщения не найдены')
        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')
