import asyncio
from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.utils.validate import validate
from app.models.event import Event



def routes():
    return [
        Route('/event/feed', events_feed, methods = [ 'POST' ]),

        Route('/m/event/list', moderator_events_list, methods = [ 'POST' ]),
    ]



MODELS = {
    # moderator
	'moderator_events_list': {
        'page': {
            'required': True,
            'type': 'int',
            'value_min': 1,
            'default': 1,
        },
	},
}



################################################################
async def events_feed(request):
    if request.user.id:
        result = await Event.list()
        events_ids = [ item.id for item in result ]
        events_ids_selected = await request.user.filter_selected_events(events_ids)
        events_ids_thumbsup = await request.user.filter_thumbsup(events_ids)
        return OrjsonResponse({
            'events': [ item.show() for item in result ],
            'events_selected': { str(id): True for id in events_ids_selected },
            'events_thumbsup': { str(id): True for id in events_ids_thumbsup },
        })
    else:
        return err(403, 'Нет доступа')



################################################################
async def moderator_events_list(request):
    if request.user.id and request.user.check_roles({ 'admin', 'editor' }):
        if validate(request.params, MODELS['moderator_events_list']):
            result = await Event.list()
            i = (request.params['page'] - 1) * 10
            return OrjsonResponse({
                'events': [ item.show() for item in result[i:i + 10] ],
                'amount': len(result),
            })

        else:
            return err(400, 'Неверный поиск')
    else:
        return err(403, 'Нет доступа')
