from starlette.routing import Route


from app.core.context import get_api_context
from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.suggestions import get_suggestions_list, get_suggestions_comments
from app.models.user import get_community_managers



def routes():
    return [
        Route('/ma/suggestions/control', manager_get_suggestions, methods = [ 'POST' ]),
    ]



MODELS = {
    'manager_get_suggestions': {
        'page': {
            'required': True,
            'type': 'int',
            'value_min': 1,
            'default': 1,
        },
        'community_manager_id': {
            'required': True,
            'type': 'int',
            'value_min': 0,
            'null': True,
        },
        'evaluation': {
            'required': True,
            'type': 'bool',
            'list': True,
        },
        'date_evaluation': {
            'required': True,
            'type': 'int',
            'null': True,
        },
	},
}



################################################################
async def manager_get_suggestions(request):
    if request.user.id and request.user.check_roles({ 'admin', 'moderator', 'manager', 'chief' }):
        if validate(request.params, MODELS['manager_get_suggestions']):
            community_manager_id = request.params['community_manager_id']
            data = await get_suggestions_list(
                page = request.params['page'],
                community_manager_id = community_manager_id,
                evaluation = request.params['evaluation'],
                date_evaluation = request.params['date_evaluation'],
            )
            comments = {}
            if data[1]:
                ids = []
                for item in data[1]:
                    ids.append(( item['user_1_id'], item['user_2_id'] ))
                comments = await get_suggestions_comments(ids = ids)
            community_managers = await get_community_managers()
            return OrjsonResponse({
                'amount': data[0],
                'suggestions': data[1],
                'comments': comments,
                'page': data[2],
                'community_managers': community_managers,
            })
        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')
