from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.poll import Poll
from app.models.community import Community, get_data_for_select
from app.models.notification import create_notifications



def routes():
    return [
        Route('/poll/add/vote', poll_add_vote, methods = [ 'POST' ]),

        Route('/m/poll/list', moderator_poll_list, methods = [ 'POST' ]),
        Route('/m/poll/update', moderator_poll_update, methods = [ 'POST' ]),
        Route('/m/poll/create', moderator_poll_create, methods = [ 'POST' ]),
        Route('/m/poll/log', moderator_poll_log, methods = [ 'POST' ]),
    ]



MODELS = {
    'poll_add_vote': {
        'poll_id': {
            'required': True,
			'type': 'int',
            'value_min': 1,
        },
        'answer': {
            'required': True,
			'type': 'int',
            'value_min': 1,
        },
    },
    'moderator_poll_update': {
		'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'active': {
			'required': True,
			'type': 'bool',
		},
        'closed': {
			'required': True,
			'type': 'bool',
		},
        'wide': {
			'required': True,
			'type': 'bool',
		},
		'text': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'community_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
		'answers': {
			'required': True,
			'type': 'str',
            'list': True,
            'null': True,
		},
		'tags': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
            'null': True,
		},
        'history': {
			'required': True,
			'type': 'str',
            'list': True,
            'null': True,
		},
	},
    'moderator_poll_create': {
        'active': {
			'required': True,
			'type': 'bool',
		},
        'closed': {
			'required': True,
			'type': 'bool',
		},
        'wide': {
			'required': True,
			'type': 'bool',
		},
		'text': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'community_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
		'answers': {
			'required': True,
			'type': 'str',
            'list': True,
            'null': True,
		},
		'tags': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
            'null': True,
		},
	},
    'moderator_poll_log': {
		'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
}



################################################################
async def poll_add_vote(request):
    if request.user.id:
        if validate(request.params, MODELS['poll_add_vote']):
            poll = Poll()
            await poll.set(id = request.params['poll_id'])
            if poll.id and request.params['answer'] <= len(poll.answers) and poll.closed is False and poll.active is True:
                await poll.add_vote(request.user.id, request.params['answer'])
                dispatch('poll_update', request)
                return OrjsonResponse({})
            else:
                return err(404, 'Опрос не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_poll_list(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        polls = await Poll.search()   
        result = []
        for poll in polls:
            result.append(poll.show())
        select = await get_data_for_select()
        return OrjsonResponse({
            'polls': result,
            'select': select,
        })
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_poll_update(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        if validate(request.params, MODELS['moderator_poll_update']):
            poll = Poll()
            await poll.set(id = request.params['id'])
            notify = False
            if 'active' in request.params and 'closed' in request.params and \
                    request.params['active'] is True and request.params['closed'] is False and \
                    (poll.active is False or poll.closed is True):
                notify = True
            if poll.id:
                await poll.update(**request.params)
                await poll.set(id = request.params['id'])
                dispatch('poll_update', request)
                if notify:
                    create_notifications('poll_create', request.user.id, poll.id, {
                        'id': poll.id,
                        'active': poll.active,
                        'closed': poll.closed,
                        'wide': poll.wide,
                        'tags': poll.tags,
                        'community_id': poll.community_id,
                        'community_name': poll.community_name,
                        'text': poll.text,
                        'answers': poll.answers,
                    })
                return OrjsonResponse({})
            else:
                return err(404, 'Группа не найдена')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_poll_create(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        if validate(request.params, MODELS['moderator_poll_create']):
            community = Community()
            await community.set(id = request.params['community_id'])
            if community.id:
                poll = Poll()
                await poll.create(
                    text = request.params['text'],
                    community_id = request.params['community_id'],
                    answers = request.params['answers'],
                    tags = request.params['tags'],
                    active = request.params['active'],
                    closed = request.params['closed'],
                    wide = request.params['wide'],
                )
                dispatch('poll_create', request)
                create_notifications('poll_create', request.user.id, poll.id, {
                    'id': poll.id,
                    'active': poll.active,
                    'closed': poll.closed,
                    'wide': poll.wide,
                    'tags': poll.tags,
                    'community_id': poll.community_id,
                    'community_name': poll.community_name,
                    'text': poll.text,
                    'answers': poll.answers,
                })
                return OrjsonResponse({})
            else:
                return err(404, 'Сообщество не найдено')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_poll_log(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        if validate(request.params, MODELS['moderator_poll_log']):
            poll = Poll()
            await poll.set(id = request.params['id'])
            if poll.id:
                log = await poll.get_votes_log()
                return OrjsonResponse({
                    'poll': poll.show(),
                    'log': log,
                })
            else:
                return err(404, 'Опрос не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')
