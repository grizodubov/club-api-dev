import asyncio
from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.utils.validate import validate
from app.models.event import Event, get_participants
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
        result = []
        for event_id in events_ids:
            event = Event()
            await event.set(id = event_id)
            result.append(event)
        events_selected = await request.user.filter_selected_events(events_ids)
        items_ids_thumbsup = await request.user.filter_thumbsup(events_ids)
        events = []
        participants = await get_participants(events_ids = events_ids)
        for event in result:
            k = str(event.id)
            event_participants = { 'participants': participants[k] if k in participants else [] }
            events.append(
                event.show() | event_participants
            )

        return OrjsonResponse({
            'items': events,
            'events_selected': { str(item['event_id']): item['confirmation'] for item in events_selected },
            'items_thumbsup': { str(id): True for id in items_ids_thumbsup },
        })
    else:
        return err(403, 'Нет доступа')
