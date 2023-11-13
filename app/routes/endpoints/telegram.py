import re
from starlette.routing import Route

from app.core.request import err
from app.core.response import OrjsonResponse
from app.core.event import dispatch
from app.models.user import User

import pprint



def routes():
    return [
        Route('/hook/telegram/message', telegram_message, methods = [ 'POST' ]),
    ]



################################################################
async def telegram_message(request):
    api = get_api_context()
    print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
    print('TELEGRAM WEBHOOK')
    print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
    # pprint.pprint(request.params)
    # Отвязка телеграм
    # if 'my_chat_member' in request.params and \
    #         'new_chat_member' in request.params['my_chat_member'] and request.params['my_chat_member']['new_chat_member']['status'] == 'kicked' and \
    #         'chat' in request.params['my_chat_member'] and 'id' in request.params['my_chat_member']['chat'] and request.params['my_chat_member']['chat']['id']:
    #     await unlink_telegram_chat_id(api, str(request.params['my_chat_member']['chat']['id']))
    # Привязка телеграм
    if 'message' in request.params and \
            'text' in request.params['message'] and request.params['message']['text'] and \
            'chat' in request.params['message'] and 'id' in request.params['message']['chat'] and request.params['message']['chat']['id']:
        if request.params['message']['chat']['id'] == request.params['message']['from']['id'] and \
                request.params['message']['from']['is_bot'] is False:
            # pprint.pprint(request.params)
            message = request.params['message']['text']
            print('message', message)
            if message != '/start':
                m = re.match(r'^\D*(\d{6})\D*$', message)
                if m:
                    pin = str(m.group(1))
                    chat_id = str(request.params['message']['chat']['id'])
                    id = await api.redis.data.get(str('__TELEGRAM__' + pin))
                    if id:
                        user_id = int(id)
                        user = User()
                        await user.set(id = user_id)
                        if user.id:
                            await user.update_telegram(chat_id)
                            # send_message(api.stream_telegram, chat_id, 'Спасибо! Телеграм-чат привязан к Digitender. Теперь Вы будете получать сообщения об изменениях в Вашем личном кабинете в системе Digitender.')
                            dispatch('user_update', request)
                        return OrjsonResponse({
                            'action': 'link',
                            'success': True,
                        })
    return OrjsonResponse({
        'success': False,
    })
