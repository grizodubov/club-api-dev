import pymorphy3

from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.community import Community, get_stats, get_posts, sort_communities, add_post, update_post, check_post, check_question, check_answer, find_questions, extra_update_post, extra_delete_post
from app.models.user import User
from app.models.item_ import Items



def routes():
    return [
        Route('/community/list', community_list, methods = [ 'POST' ]),
        Route('/community/post/add', community_add_post, methods = [ 'POST' ]),
        Route('/community/post/update', community_update_post, methods = [ 'POST' ]),
        Route('/community/suggestions', community_suggestions, methods = [ 'POST' ]),

        Route('/m/community/search', moderator_community_search, methods = [ 'POST' ]),
        Route('/m/community/questions', moderator_community_questions, methods = [ 'POST' ]),
        Route('/m/community/update', moderator_community_update, methods = [ 'POST' ]),
        Route('/m/community/create', moderator_community_create, methods = [ 'POST' ]),
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
            'value_min': 1,
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
        communities = Items()
        await communities.search('community')
        communities_ids = [ item['id'] for item in communities.items ]
        stats = await get_stats(communities_ids, request.user.id)
        communities_sorted = sort_communities(communities.items, stats)
        community_id = None
        if request.params['community_id'] and request.params['community_id'] in communities_ids:
            community_id = request.params['community_id']
        else:
            if communities_sorted:
                community_id = communities_sorted[0]['id']
        posts = await get_posts(community_id, request.user.id)
        communities_full = []
        for community in communities_sorted:
            cm = Community()
            await cm.set(id = community['id'])
            communities_full.append(
                cm.show()
            )
        return OrjsonResponse({
            'communities': communities_full,
            'stats': stats,
            'community_id': community_id,
            'posts': posts,
        })
    else:
        return err(403, 'Нет доступа')



################################################################
async def community_add_post(request):
    if request.user.id:
        if validate(request.params, MODELS['community_add_post']):
            community = Community()
            await community.set(id = request.params['community_id'])
            if community.id:
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
async def moderator_community_questions(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        if validate(request.params, MODELS['moderator_community_questions']):
            community = Community()
            await community.set(id = request.params['id'])
            if community.id:
                posts = await get_posts(request.params['id'], request.user.id)
                return OrjsonResponse({
                    'community': community.show(),
                    'posts': posts,
                })
            else:
                return err(404, 'Сообщество не найдено')
        else:
            return err(400, 'Неверный запрос')
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
            )
            dispatch('community_create', request)
            return OrjsonResponse({})
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_community_post_update(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        if validate(request.params, MODELS['moderator_community_post_update']):
            result = await extra_update_post(request.params['id'], request.params)
            if result:
                dispatch('post_update', request)
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
