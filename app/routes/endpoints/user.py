import asyncio
from random import randint
from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.utils.validate import validate



def routes():
    return [
        Route('/user/summary', user_summary, methods = [ 'POST' ]),
        Route('/user/contacts', user_contacts, methods = [ 'POST' ]),
    ]



MODELS = {
}



################################################################
async def user_summary(request):
    if request.user.id:
        result = await request.user.get_summary()
        result['amounts_messages'] = await request.user.get_unread_messages_amount()
        return OrjsonResponse(result)
    else:
        return err(403, 'Нет доступа')



################################################################
async def user_contacts(request):
    if request.user.id:

        
        return OrjsonResponse(result)
    else:
        return err(403, 'Нет доступа')