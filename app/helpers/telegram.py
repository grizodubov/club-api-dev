import aiohttp
import orjson



################################################################
def dumps(*args):
    return orjson.dumps(args[0]).decode()



################################################################
def send_telegram_message(stream, chat_id, body):
    params = {
        'chat_id': chat_id,
        'text': body,
        'parse_mode': 'HTML',
        # 'parse_mode': '',

    }
    stream.register(
        send_to_telegram,
        params = params,
    )



################################################################
async def send_to_telegram(params):
    resp = None
    async with aiohttp.ClientSession(json_serialize = dumps) as session:
        try:
            async with session.post('https://api.telegram.org/bot6532384944:AAHwgY9JA8ZQAsa89BwxHJzERNhaVorqVDg/sendMessage',
                json = params,
            ) as response:
                if response.status == 200:
                    resp = await response.read()
                else:
                    print('TELEGRAM: RESPONSE ERROR:', response.status, await response.read(), params)
                    if response.status == 403:
                        raise Exception('Telegram link error: ' + str(params['chat_id']))
                    if response.status == 500:
                        raise Exception('Telegram response error')
        except aiohttp.ClientError as e:
            print('TELEGRAM: CONNECTION ERROR:', e)
            raise Exception('Telegram connection error')
    data = None
    if resp:
        try:
            data = orjson.loads(resp)
        except orjson.JSONDecodeError as e:
            print('TELEGRAM: DATA ERROR:', e)
            raise Exception('Telegram data error')
    print('MESSAGE TO TELEGRAM', data)
