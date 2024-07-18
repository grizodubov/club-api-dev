from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.tag import get_tags, update_tag, tag_1_get_categories, tag_1_get_category, tag_1_get_catalog, tag_1_get_tags, tag_1_category_update, tag_1_catalog_add_tag, tag_1_catalog_update_tag, tag_1_catalog_delete_tag, tag_1_user_update_tag, tag_1_user_delete_tag, tag_1_user_move_tag



def routes():
    return [
        Route('/tag/list', list_tags, methods = [ 'POST' ]),
        Route('/m/tag/list', moderator_list_tags, methods = [ 'POST' ]),
        Route('/m/tag/replace', moderator_replace_tag, methods = [ 'POST' ]),
        Route('/m/tag/delete', moderator_delete_tag, methods = [ 'POST' ]),
        # tag1
        Route('/m/tag1/categories/list', moderator_tag1_categories, methods = [ 'POST' ]),
        Route('/m/tag1/tags/list', moderator_tag1_tags, methods = [ 'POST' ]),
        Route('/m/tag1/category/update', moderator_tag1_category_update, methods = [ 'POST' ]),
        Route('/m/tag1/catalog/tag', moderator_tag1_catalog_tag, methods = [ 'POST' ]),
        Route('/m/tag1/user/tag', moderator_tag1_user_tag, methods = [ 'POST' ]),
        Route('/m/tag1/user/tag/move', moderator_tag1_user_tag_move, methods = [ 'POST' ]),
    ]



MODELS = {
    'moderator_replace_tag': {
		'tag': {
			'required': True,
			'type': 'str',
		},
		'tag_new': {
			'required': True,
			'type': 'str',
		},
	},
    'moderator_delete_tag': {
		'tag': {
			'required': True,
			'type': 'str',
		},
	},
    # tag1
    'moderator_tag1_tags': {
        'category': {
            'required': True,
			'type': 'str',
        },
    },
    'moderator_tag1_category_update': {
        'category': {
            'required': True,
			'type': 'str',
        },
        'allow_user_tags': {
            'required': True,
			'type': 'bool',
        },
    },
    'moderator_tag1_catalog_tag': {
        'catalog': {
            'required': True,
			'type': 'str',
        },
        'id': {
            'required': True,
			'type': 'int',
            'null': True,
        },
        'method': {
            'required': True,
			'type': 'str',
            'values': [ 'add', 'upd', 'del' ],
        },
        'tag': {
            'required': True,
			'type': 'str',
        },
    },
    'moderator_tag1_user_tag': {
        'category': {
            'required': True,
			'type': 'str',
        },
        'method': {
            'required': True,
			'type': 'str',
            'values': [ 'upd', 'del' ],
        },
        'tag': {
            'required': True,
			'type': 'str',
            'list': True,
        },
        'tag_new': {
            'required': True,
			'type': 'str',
        },
    },
    'moderator_tag1_user_tag_move': {
        'category': {
            'required': True,
			'type': 'str',
        },
        'tag': {
            'required': True,
			'type': 'str',
            'list': True,
        },
        'parent_id': {
            'required': True,
			'type': 'str',
        },
    },
}



################################################################
async def list_tags(request):
    if request.user.id:
        tags = await get_tags()
        return OrjsonResponse({
            'tags': [
                {
                    'tag': list(v['options'])[0],
                    'competency': v['competency'],
                    'interests': v['interests'],
                } for v in tags.values()
            ]
        })
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_list_tags(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager', 'chief', 'agent' }):
        tags = await get_tags()
        return OrjsonResponse({
            'tags': [
                {
                    'tag': k,
                    'options': list(v['options']),
                    'competency': v['competency'],
                    'interests': v['interests'],
                    'communities': v['communities'],
                } for k, v in tags.items()
            ]
        })
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_replace_tag(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        if validate(request.params, MODELS['moderator_replace_tag']):
            await update_tag(request.params['tag'], request.params['tag_new'])
            dispatch('tag_update', request)
            return OrjsonResponse({})
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_delete_tag(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        if validate(request.params, MODELS['moderator_delete_tag']):
            await update_tag(request.params['tag'], '')
            dispatch('tag_update', request)
            return OrjsonResponse({})
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_tag1_categories(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        categories = await tag_1_get_categories()
        return OrjsonResponse({
            'categories': categories,
        })
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_tag1_tags(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'community manager' }):
        if validate(request.params, MODELS['moderator_tag1_tags']):
            category = await tag_1_get_category(request.params['category'])
            if category:
                catalog = None
                if category['catalog']:
                    catalog = await tag_1_get_catalog(category['catalog'])
                tags = await tag_1_get_tags(request.params['category'])
                temp_tags = { t['tag_full'].lower(): t['tag_full'] for t in catalog } if catalog else {}
                for k, v in tags.items():
                    v['options'] = list(v['options'])
                    if k in temp_tags:
                        v['tag_in_catalog'] = temp_tags[k]
                    else:
                        v['tag_in_catalog'] = None
                tags_sorted = sorted([ v | { 'tag_key': k, 'users_ids': list(v['users_ids']) } for k, v in tags.items() ], key = lambda t: t['tag_key'])
                if catalog:
                    for v in catalog:
                        v.update({ 
                            'tag_key': v['tag'].lower(),
                            'tag_key_full': v['tag_full'].lower(),
                        })
                        v['users_ids'] = tags[v['tag_key_full']]['users_ids'] if v['tag_key_full'] in tags else set()
                    for v in catalog:
                        if v['parent_id'] is None:
                            v['users_ids'].update(count_children_users(catalog, v))
                    for v in catalog:
                        v['amount'] = len(v['users_ids'])
                        del v['users_ids']
                return OrjsonResponse({
                    'category': category,
                    'catalog': catalog,
                    'tags': tags_sorted,
                })
            else:
                return err(404, 'Категория не найдена')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
def count_children_users(catalog, item):
    result = set()
    for v in catalog:
        if v['parent_id'] == item['id']:
            v['users_ids'].update(count_children_users(catalog, v))
            result.update(v['users_ids'])
    return result



################################################################
async def moderator_tag1_category_update(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator' }):
        if validate(request.params, MODELS['moderator_tag1_category_update']):
            category = await tag_1_get_category(request.params['category'])
            if category:
                await tag_1_category_update(request.params['category'], request.params['allow_user_tags'])
                return OrjsonResponse({})
            else:
                return err(404, 'Категория не найдена')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_tag1_catalog_tag(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator' }):
        if validate(request.params, MODELS['moderator_tag1_catalog_tag']):
            if request.params['method'] == 'add':
                await tag_1_catalog_add_tag(request.params['id'], request.params['catalog'], request.params['tag'])
            elif request.params['method'] == 'upd':
                await tag_1_catalog_update_tag(request.params['id'], request.params['tag'])
            elif request.params['method'] == 'del':
                await tag_1_catalog_delete_tag(request.params['id'])
            return OrjsonResponse({})
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_tag1_user_tag(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator' }):
        if validate(request.params, MODELS['moderator_tag1_user_tag']):
            if request.params['method'] == 'upd':
                await tag_1_user_update_tag(request.params['category'], request.params['tag'], request.params['tag_new'])
            elif request.params['method'] == 'del':
                await tag_1_user_delete_tag(request.params['category'], request.params['tag'])
            return OrjsonResponse({})
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_tag1_user_tag_move(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator' }):
        if validate(request.params, MODELS['moderator_tag1_user_tag_move']):
            await tag_1_user_move_tag(request.params['category'], request.params['tag'], request.params['parent_id'])
            return OrjsonResponse({})
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')
