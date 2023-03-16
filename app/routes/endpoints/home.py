from starlette.routing import Route

from app.core.response import OrjsonResponse



def routes():
    return [
        Route('/', homepage, methods = [ 'GET' ]),
        Route('/', homepage, methods = [ 'POST' ]),
    ]



async def homepage(request):
    return OrjsonResponse({
        'api': 'club',
        'description': 'Закрытый клуб для поставщиков товаров и услуг на тендерном рынке.',
    })
