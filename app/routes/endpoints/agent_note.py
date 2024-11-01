import asyncio
from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.agent_note import Note
from app.models.user import User



def routes():
    return [
        Route('/ma/agent/note/list', manager_agents_notes_list, methods = [ 'POST' ]),
        Route('/ma/agent/note/update', manager_agents_notes_update, methods = [ 'POST' ]),
        Route('/ma/agent/note/create', manager_agents_notes_create, methods = [ 'POST' ]),
    ]



MODELS = {
    # manager
	'manager_agents_notes_list': {
        'user_id': {
            'required': True,
            'type': 'int',
            'value_min': 1,
        },
	},
	'manager_agents_notes_update': {
		'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
		'note': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
		},
	},
	'manager_agents_notes_create': {
		'note': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
        'user_id': {
            'required': True,
            'type': 'int',
            'value_min': 1,
        },
	},
}



################################################################
async def manager_agents_notes_list(request):
    if request.user.id and request.user.check_roles({ 'admin', 'editor', 'manager', 'chief', 'community manager', 'agent' }):
        if validate(request.params, MODELS['manager_agents_notes_list']):
            user = User()
            await user.set(id = request.params['user_id'])
            if user.id:
                result = await Note.list(user_id = user.id)
                return OrjsonResponse({
                    'notes': [ item.show() for item in result],
                })
            else:
                return err(404, 'Пользователь не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_agents_notes_update(request):
    if request.user.id and request.user.check_roles({ 'admin', 'editor', 'manager', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_agents_notes_update']):
            note = Note()
            await note.set(id = request.params['id'])
            if note.id:
                if note.author_id == request.user.id:
                    await note.update(note = request.params['note'])
                    dispatch('agent_note_update', request)
                    return OrjsonResponse({})
                else:
                    return err(403, 'Нет доступа')
            else:
                return err(404, 'Заметка не найдена')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def manager_agents_notes_create(request):
    if request.user.id and request.user.check_roles({ 'admin', 'editor', 'manager', 'chief', 'community manager' }):
        if validate(request.params, MODELS['manager_agents_notes_create']):
            user = User()
            await user.set(id = request.params['user_id'])
            if user.id:
                note = Note()
                await note.create(note = request.params['note'], user_id = user.id, author_id = request.user.id)
                dispatch('agent_note_create', request)
                return OrjsonResponse({})
            else:
                return err(404, 'Пользователь не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')
