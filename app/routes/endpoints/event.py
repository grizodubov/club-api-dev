import asyncio
from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.utils.validate import validate
from app.models.event import Event



def routes():
    return [
        Route('/event/feed', events_feed, methods = [ 'POST' ]),
    ]



MODELS = {
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
