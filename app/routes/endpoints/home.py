from starlette.routing import Route

from app.core.response import OrjsonResponse



def routes():
    return [
        Route('/', homepage, methods = [ 'GET' ]),
        Route('/', homepage, methods = [ 'POST' ]),
        Route('/acquire', acquire, methods = [ 'POST' ]),
    ]



def homepage(request):
    return OrjsonResponse({
        'api': 'club',
        'description': 'Закрытый клуб для поставщиков товаров и услуг на тендерном рынке.',
    })



def acquire(request):
    return OrjsonResponse({
        'api': 'https://eta.clubgermes.ru:5007',
        'wso': 'wss://eta.clubgermes.ru:5007',
    })
