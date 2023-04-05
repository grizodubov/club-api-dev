import asyncio
from random import randint
from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.item import Item
from app.models.message import get_chats, get_messages, add_message



def routes():
    return [
        Route('/chat/list', chats_list, methods = [ 'POST' ]),
        Route('/message/list', messages_list, methods = [ 'POST' ]),
        Route('/message/add', message_add, methods = [ 'POST' ]),
    ]



MODELS = {
	'chats_list': {
		'chat_id': {
            'required': True,
			'type': 'int',
            'value_min': 1,
            'null': True,
		},
	},
	'messages_list': {
		'chat_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
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
        }
	},
}



################################################################
async def chats_list(request):
    if request.user.id:
        if validate(request.params, MODELS['chats_list']):
            return OrjsonResponse({
                'chats': await get_chats(request.user.id)
            })
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def messages_list(request):
    if request.user.id:
        if validate(request.params, MODELS['messages_list']):
            item = Item()
            await item.set(id = request.params['chat_id'])
            if item.id:
                access = True
                if item.model == 'group':
                    access = await request.user.group_access(group_id = item.id)
                if access:
                    vector = None
                    if request.params['vector_type'] != 'init':
                        vector = {
                            'reverse': request.params['vector_type'] == 'reverse',
                            'id': request.params['vector_id'],
                        }
                    return OrjsonResponse({
                        'messages': await get_messages(
                            user_id = request.user.id,
                            chat_id = item.id,
                            chat_model = item.model,
                            init = request.params['vector_type'] == 'init',
                            vector = vector,
                        )
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
async def message_add(request):
    if request.user.id:
        if validate(request.params, MODELS['message_add']):
            item = Item()
            await item.set(id = request.params['chat_id'])
            if item.id:
                access = True
                if item.model == 'group':
                    access = await request.user.group_access(group_id = item.id)
                if access:
                    message = await add_message(
                        user_id = request.user.id,
                        chat_id = item.id,
                        chat_model = item.model,
                        text = request.params['text']
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
