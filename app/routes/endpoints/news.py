import asyncio
from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.news import News



def routes():
    return [
        Route('/m/news/list', moderator_news_list, methods = [ 'POST' ]),
        Route('/m/news/update', moderator_news_update, methods = [ 'POST' ]),
        Route('/m/news/create', moderator_news_create, methods = [ 'POST' ]),
    ]



MODELS = {
    # moderator
	'moderator_news_list': {
        'page': {
            'required': True,
            'type': 'int',
            'value_min': 1,
            'default': 1,
        },
	},
	'moderator_news_update': {
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
		'detail': {
			'required': True,
			'type': 'str',
		},
	},
	'moderator_news_create': {
		'name': {
			'required': True,
			'type': 'str',
            'length_min': 2,
            'processing': lambda x: x.strip(),
		},
		'detail': {
			'required': True,
			'type': 'str',
		},
	},
}



################################################################
async def moderator_news_list(request):
    if request.user.id and request.user.check_roles({ 'admin', 'editor' }):
        if validate(request.params, MODELS['moderator_news_list']):
            result = await News.list()
            i = (request.params['page'] - 1) * 10
            return OrjsonResponse({
                'news': [ item.show() for item in result[i:i + 10] ],
                'amount': len(result),
            })

        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_news_update(request):
    if request.user.id and request.user.check_roles({ 'admin', 'editor' }):
        if validate(request.params, MODELS['moderator_news_update']):
            news = News()
            await news.set(id = request.params['id'])
            if news.id:
                await news.update(**request.params)
                dispatch('news_update', request)
                return OrjsonResponse({})
            else:
                return err(404, 'Группа не найдена')
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_news_create(request):
    if request.user.id and request.user.check_roles({ 'admin', 'editor' }):
        if validate(request.params, MODELS['moderator_news_create']):
            news = News()
            await news.create(**request.params)
            dispatch('news_create', request)
            return OrjsonResponse({})
        else:
            return err(400, 'Неверный запрос')
    else:
        return err(403, 'Нет доступа')
