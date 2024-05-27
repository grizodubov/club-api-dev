from datetime import datetime
import pytz

from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.poll import Poll, get_user_rating_polls
from app.models.community import Community, get_data_for_select
from app.models.notification import create_notifications



def routes():
    return [
        Route('/poll/add/vote', poll_add_vote, methods = [ 'POST' ]),
        Route('/poll/rating/list', poll_rating_list, methods = [ 'POST' ]),

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
            'list': True,
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
            'null': True,
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
        'rating': {
			'required': True,
			'type': 'bool',
		},
        'rating_format': {
			'required': True,
			'type': 'str',
            'null': True,
		},
        'many': {
			'required': True,
			'type': 'bool',
		},
        'score': {
			'required': True,
			'type': 'int',
		},
        'show_results': {
			'required': True,
			'type': 'bool',
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
            'null': True,
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
        'rating': {
			'required': True,
			'type': 'bool',
		},
        'rating_format': {
			'required': True,
			'type': 'str',
            'null': True,
		},
        'many': {
			'required': True,
			'type': 'bool',
		},
        'score': {
			'required': True,
			'type': 'int',
		},
        'show_results': {
			'required': True,
			'type': 'bool',
		},
	},
    'moderator_poll_log': {
		'id': {
			'required': True,
			'type': 'int',
            'value_min': 0,
		},
	},
}



################################################################
async def poll_add_vote(request):
    if request.user.id:
        if validate(request.params, MODELS['poll_add_vote']):
            poll = Poll()
            await poll.set(id = request.params['poll_id'])
            check = True
            for answer in request.params['answer']:
                if answer > len(poll.answers):
                    check = False
                    break
            if poll.id and check and poll.closed is False and poll.active is True:
                votes = await poll.get_votes_log()
                user_vote_time = None
                for vote in votes:
                    if vote['user_id'] == request.user.id:
                        if user_vote_time is None or vote['time'] > user_vote_time:
                            user_vote_time = vote['time']
                if user_vote_time:
                    if poll.rating_format == 'Каждый месяц':
                        dt_now = datetime.now(tz = pytz.utc)
                        dt = dt_now.timestamp() * 1000 - user_vote_time
                        if dt < 1728000000:
                            return err(404, 'Голос уже принят')
                    else:
                        return err(404, 'Голос уже принят')
                await poll.add_vote(request.user.id, request.params['answer'])
                dispatch('poll_update', request)
                return OrjsonResponse({
                    '_notification': 'Спасибо за Ваш голос!'
                })
            else:
                return err(404, 'Опрос не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def poll_rating_list(request):
    if request.user.id:
        result = {
            'polls': [],
            'votes': [],
        }
        if request.user.check_roles({ 'client' }):
            result = await get_user_rating_polls(request.user)
        return OrjsonResponse(result)
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_poll_list(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        polls = await Poll.search()   
        result = []
        for poll in polls:
            result.append(poll.show_with_score())
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
            rating = True if 'rating' in request.params and request.params['rating'] is True else False
            if not rating:
                community = Community()
                if request.params['community_id']:
                    await community.set(id = request.params['community_id'])
                if not community.id:
                    return err(404, 'Сообщество не найдено')
            poll = Poll()
            await poll.set(id = request.params['id'])
            temp = {
                'active': poll.active,
                'closed': poll.closed,
                'rating': poll.rating,
            }
            if poll.id:
                await poll.update(**request.params)
                await poll.set(id = request.params['id'])
                dispatch('poll_update', request)
                # notify
                if not rating and \
                        'active' in request.params and 'closed' in request.params and \
                        request.params['active'] is True and request.params['closed'] is False and \
                        (temp['active'] is False or temp['closed'] is True or temp['rating'] is True):
                    create_notifications('poll_create', request.user.id, poll.id, {
                        'id': poll.id,
                        'active': poll.active,
                        'closed': poll.closed,
                        'wide': poll.wide,
                        'show_results': poll.show_results,
                        'tags': poll.tags,
                        'community_id': poll.community_id,
                        'community_name': poll.community_name,
                        'text': poll.text,
                        'answers': poll.answers,
                    })
                return OrjsonResponse({})
            else:
                return err(404, 'Опрос не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_poll_create(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        if validate(request.params, MODELS['moderator_poll_create']):
            rating = True if 'rating' in request.params and request.params['rating'] is True else False
            if not rating:
                community = Community()
                if request.params['community_id']:
                    await community.set(id = request.params['community_id'])
                if not community.id:
                    return err(404, 'Сообщество не найдено')
            poll = Poll()
            await poll.create(
                text = request.params['text'],
                community_id = request.params['community_id'] if not rating else None,
                answers = request.params['answers'],
                tags = request.params['tags'] if not rating else '',
                rating = rating,
                rating_format = request.params['rating_format'] if rating else None,
                active = request.params['active'],
                closed = request.params['closed'],
                wide = request.params['wide'],
                many = request.params['many'],
            )
            dispatch('poll_create', request)
            # notify
            if not rating and \
                    'active' in request.params and 'closed' in request.params and \
                    request.params['active'] is True and request.params['closed'] is False:
                create_notifications('poll_create', request.user.id, poll.id, {
                    'id': poll.id,
                    'active': poll.active,
                    'closed': poll.closed,
                    'wide': poll.wide,
                    'tags': poll.tags,
                    'community_id': poll.community_id,
                    'community_name': poll.community_name,
                    'show_results': poll.show_results,
                    'text': poll.text,
                    'answers': poll.answers,
                })
            if rating and \
                    'active' in request.params and 'closed' in request.params and \
                    request.params['active'] is True and request.params['closed'] is False:
                create_notifications('rating_poll_create', request.user.id, poll.id, {
                    'text': poll.text,
                })
            return OrjsonResponse({})
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
                    'poll': poll.show_with_score(),
                    'log': log,
                })
            else:
                return err(404, 'Опрос не найден')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')
