import pymorphy3

from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.community import Community, get_stats, get_posts, sort_communities, add_post, update_post, move_post, check_post, check_question, check_answer, find_questions, extra_update_post, extra_delete_post, get_data_for_select, get_unverified_questions, get_verified_flag, get_user_questions, get_user_recommendations, get_active_communities
from app.models.user import User
from app.models.poll import Poll, get_user_polls_recommendations
from app.models.item_ import Items
from app.models.notification import create_notifications



def routes():
    return [
        Route('/community/list', community_list, methods = [ 'POST' ]),
        Route('/community/post/add', community_add_post, methods = [ 'POST' ]),
        Route('/community/post/update', community_update_post, methods = [ 'POST' ]),
        Route('/community/suggestions', community_suggestions, methods = [ 'POST' ]),
        Route('/community/questions/user', community_questions_top, methods = [ 'POST' ]),

        Route('/m/community/search', moderator_community_search, methods = [ 'POST' ]),
        Route('/m/community/questions', moderator_community_questions, methods = [ 'POST' ]),
        Route('/m/community/questions/unverified', moderator_questions_unverified, methods = [ 'POST' ]),
        Route('/m/community/update', moderator_community_update, methods = [ 'POST' ]),
        Route('/m/community/create', moderator_community_create, methods = [ 'POST' ]),
        Route('/m/community/question/move', moderator_community_question_move, methods = [ 'POST' ]),
        Route('/m/community/post/update', moderator_community_post_update, methods = [ 'POST' ]),
        Route('/m/community/post/delete', moderator_community_post_delete, methods = [ 'POST' ]),
    ]



MODELS = {
	'community_list': {
		'community_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
            'null': True,
		},
	},
	'community_add_post': {
		'community_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
            'null': True,
		},
        'text': {
            'required': True,
			'type': 'str',
            'length_min': 1,
        },
		'reply_to_post_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
            'null': True,
		},
	},
	'community_update_post': {
		'post_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'closed': {
            'required': False,
            'type': 'bool',
            'null': True,
        },
        'helpful': {
            'required': False,
            'type': 'bool',
            'null': True,
        },
	},
    'community_suggestions': {
        'text': {
            'required': True,
			'type': 'str',
            'length_min': 5,
        },
		'community_id': {
			'required': True,
			'type': 'int',
            'value_min': 0,
		},
    },
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
	'moderator_community_questions': {
		'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
	'moderator_community_update': {
		'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
        'active': {
			'required': True,
			'type': 'bool',
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
		'parent_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
            'null': True,
		},
		'members': {
			'required': True,
			'type': 'int',
            'list': True,
            'null': True,
		},
		'tags': {
			'required': True,
			'type': 'str',
            'processing': lambda x: x.strip(),
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
		'parent_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
            'null': True,
		},
	},
	'moderator_community_question_move': {
		'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
		'community_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
    'moderator_community_post_update': {
		'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
		'text': {
			'required': True,
			'type': 'str',
            'length_min': 1,
            'processing': lambda x: x.strip(),
		},
		'helpful': {
			'required': True,
			'type': 'bool',
		},
		'closed': {
			'required': True,
			'type': 'bool',
		},
		'verified': {
			'required': True,
			'type': 'bool',
		},
		'tags': {
			'required': True,
			'type': 'str',
            'null': True,
		},
		'community_id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
	'moderator_community_post_delete': {
		'id': {
			'required': True,
			'type': 'int',
            'value_min': 1,
		},
	},
}



################################################################
async def community_list(request):
    if request.user.id:
        tester = True if 'tester' in request.user.roles else False
        communities = Items()
        if tester:
            await communities.search(model = 'community')
        else:
            await communities.search(model = 'community', filter = { 'active=': True })
        communities_ids = [ item['id'] for item in communities.items ]
        #print(communities_ids, tester)
        stats = await get_stats(communities_ids, request.user.id)
        communities_sorted = sort_communities(communities.items, stats)
        community_id = None
        community_root_id = None
        if request.params['community_id'] and request.params['community_id'] in communities_ids:
            community_id = request.params['community_id']
        # else:
        #     if communities_sorted:
        #         community_id = communities_sorted[0]['id']
        posts = []
        if community_id:
            posts = await get_posts(community_id, request.user.id)
        communities_full = []
        communities_children = {}
        # TODO: blocking cycle
        for community in communities_sorted:
            cm = Community()
            await cm.set(id = community['id'])
            if cm.id == community_id and cm.parent_id:
                community_root_id = cm.parent_id
            if cm.parent_id:
                if str(cm.parent_id) in communities_children:
                    communities_children[str(cm.parent_id)].append(cm.show())
                else:
                    communities_children[str(cm.parent_id)] = [ cm.show() ]
            else:
                communities_full.append(
                    cm.show() | {
                        'children': [],
                    }
                )
        for k, v in communities_children.items():
            for community in communities_full:
                if k == str(community['id']):
                    community['children'] = v
                    break
        polls_result = []
        polls = []
        if request.params['community_id']:
            polls = await Poll.search(
                communities_ids = [ request.params['community_id'] ],
                active = True,
            )
        for poll in polls:
            temp = poll.show() | { 'answered': False, 'votes_max': 0 }
            votes = {}
            for k, v in temp['votes'].items():
                if request.user.id in v:
                    temp['answered'] = True
                votes[k] = len(v) if temp['show_results'] else 0
                if votes[k] > temp['votes_max']:
                    temp['votes_max'] = votes[k]
            temp['votes'] = votes
            polls_result.append(temp)
        return OrjsonResponse({
            'communities': communities_full,
            'stats': stats,
            'community_root_id': community_root_id,
            'community_id': community_id,
            'posts': posts,
            'polls': polls_result,
        })
    else:
        return err(403, 'Нет доступа')



################################################################
async def community_add_post(request):
    if request.user.id:
        if validate(request.params, MODELS['community_add_post']):
            community = Community()
            if request.params['community_id'] is not None:
                await community.set(id = request.params['community_id'])
            if community.id or request.params['community_id'] is None:
                result = { 'post_id': None, 'time_create': None }
                if request.params['reply_to_post_id']:
                    if await check_post(request.params['community_id'], request.params['reply_to_post_id']):
                        result = await add_post(request.params['community_id'], request.user.id, request.params['text'], request.params['reply_to_post_id'])
                    else:
                        return err(400, 'Сообщение недоступно')
                else:
                    result = await add_post(request.params['community_id'], request.user.id, request.params['text'])
                posts = await get_posts(request.params['community_id'], request.user.id)
                dispatch('post_add', request)
                if result['verified']:
                    create_notifications('post_add', request.user.id, result['id'], request.params)
                return OrjsonResponse({
                    'posts': posts,
                    'community_id': request.params['community_id'],
                    'reply_to_post_id': request.params['reply_to_post_id'],
                    'post_id': result['id'],
                    'time_create': result['time_create'],
                })
            else:
                return err(400, 'Сообщество не найдено')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def community_update_post(request):
    if request.user.id:
        if validate(request.params, MODELS['community_update_post']):
            if request.params['closed'] is not None:
                community_id = await check_question(request.params['post_id'], request.user.id)
                if community_id:
                    await update_post(request.params['post_id'], { 'closed': request.params['closed'] })
                    posts = await get_posts(community_id, request.user.id)
                    dispatch('post_update', request)
                    return OrjsonResponse({
                        'posts': posts,
                    })
                else:
                    return err(400, 'Сообщение недоступно')
            elif request.params['helpful'] is not None:
                community_id = await check_answer(request.params['post_id'], request.user.id, request.params['helpful'])
                if community_id:
                    await update_post(request.params['post_id'], { 'helpful': request.params['helpful'] })
                    posts = await get_posts(community_id, request.user.id)
                    dispatch('post_update', request)
                    return OrjsonResponse({
                        'posts': posts,
                    })
                else:
                    return err(400, 'Сообщение недоступно')
            else:
                return err(400, 'Неверный запрос')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def community_suggestions(request):
    if request.user.id:
        if validate(request.params, MODELS['community_suggestions']):
            words = request.params['text'].split()
            result = []
            if words:
                morph = pymorphy3.MorphAnalyzer(lang = 'ru')
                filter = { 'NOUN', 'ADJF', 'ADJS', 'VERB', 'INFN', 'PRTF', 'PRTS', 'GRND', 'NUMR', 'ADVB' }
                words = [ word for word in words if morph.parse(word)[0].tag.POS in filter ]
                result = await find_questions(request.params['community_id'], words)
            return OrjsonResponse({
                'words': words,
                'suggestions': result,
            })
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def community_questions_top(request):
    if request.user.id:
        questions = await get_user_questions(request.user.id)
        recommendations = await get_user_recommendations(request.user)
        polls = await get_user_polls_recommendations(request.user)
        polls_result = []
        for poll in polls:
            temp = poll.show() | { 'answered': False, 'votes_max': 0 }
            votes = {}
            for k, v in temp['votes'].items():
                votes[k] = len(v)
                if votes[k] > temp['votes_max']:
                    temp['votes_max'] = votes[k]
            temp['votes'] = votes
            polls_result.append(temp)
        return OrjsonResponse({
            'questions': questions,
            'recommendations': recommendations,
            'polls_recommendations': polls_result,
        })
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_community_questions(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        if validate(request.params, MODELS['moderator_community_questions']):
            community = Community()
            await community.set(id = request.params['id'])
            if community.id:
                posts = await get_posts(request.params['id'], request.user.id)
                select = await get_data_for_select()
                return OrjsonResponse({
                    'community': community.show(),
                    'posts': [ post for post in posts if post['question']['verified'] ],
                    'select': select,
                })
            else:
                return err(404, 'Сообщество не найдено')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_questions_unverified(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        questions = await get_unverified_questions()
        select = await get_data_for_select()
        return OrjsonResponse({
            'questions': questions,
            'select': select,
        })
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_community_search(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        if validate(request.params, MODELS['moderator_community_search']):
            (result, amount) = await Community.search(
                text = request.params['text'],
                offset = (request.params['page'] - 1) * 10,
                limit = 10,
                count = True,
                sort_active = True,
            )
            users = await User.hash()
            select = await get_data_for_select()
            return OrjsonResponse({
                'communities': [ item.dump() for item in result ],
                'users': users,
                'amount': amount,
                'select': select,
            })
        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_community_update(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
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
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        if validate(request.params, MODELS['moderator_community_create']):
            community = Community()
            await community.create(
                name = request.params['name'],
                description = request.params['description'],
                parent_id = request.params['parent_id'],
            )
            dispatch('community_create', request)
            return OrjsonResponse({})
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_community_question_move(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        if validate(request.params, MODELS['moderator_community_question_move']):
            community = Community()
            await community.set(id = request.params['community_id'])
            if community.id:
                await move_post(request.params['id'], request.params['community_id'])
                dispatch('post_update', request)
                return OrjsonResponse({})
            else:
                return err(400, 'Сообщество не найдено')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_community_post_update(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        if validate(request.params, MODELS['moderator_community_post_update']):
            notification = False
            flag = await get_verified_flag(request.params['id'])
            if 'verified' in request.params and request.params['verified'] is True and flag is False:
                notification = True
            result = await extra_update_post(request.params['id'], request.params)
            if result:
                dispatch('post_update', request)
                if notification:
                    create_notifications('post_add', request.user.id, request.params['id'], {
                        'reply_to_post_id': None,
                        'community_id': request.params['community_id'],
                    })
                return OrjsonResponse({})
            else:
                return err(400, 'Неверный запрос')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_community_post_delete(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        if validate(request.params, MODELS['moderator_community_post_delete']):
            await extra_delete_post(request.params['id'])
            dispatch('post_update', request)
            return OrjsonResponse({})
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')
