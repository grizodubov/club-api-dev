import asyncio
from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.event import Event
from app.models.feed import get_feed



def routes():
    return [
        Route('/feed', feed, methods = [ 'POST' ]),
    ]




################################################################
async def feed(request):
    if request.user.id:
        result = await get_feed()
        ids = [ item.id for item in result ]
        events_ids = [ item.id for item in result if isinstance(item, Event) ]
        events_ids_selected = await request.user.filter_selected_events(events_ids)
        items_ids_thumbsup = await request.user.filter_thumbsup(ids)
        return OrjsonResponse({
            'items': [ item.show() for item in result ],
            'events_selected': { str(id): True for id in events_ids_selected },
            'items_thumbsup': { str(id): True for id in items_ids_thumbsup },
        })
    else:
        return err(403, 'Нет доступа')
