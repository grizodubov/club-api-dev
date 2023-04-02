import aiohttp
import orjson
from jinja2 import Template



################################################################
def dumps(*args):
    return orjson.dumps(args[0]).decode()



################################################################
def send_mobile_message(stream, phone, message, data = {}):
    message_template = Template(message)
    stream.register(
        send,
        phone = phone,
        message = message_template.render(data),
    )



################################################################
async def send(phone, message):
    body = None
    async with aiohttp.ClientSession(json_serialize = dumps) as session:
        try:
            async with session.post('https://a2p-sms-https.beeline.ru/proto/http/',
                data = {
                    'user': '1684151',
                    'pass': '@H6HLSO4m',
                    'gzip': 'none',
                    'action': 'post_sms',
                    'sender': 'club_germes',
                    'message': message,
                    'target': phone,
                },
            ) as response:
                if response.status == 200:
                    body = await response.read()
                    print(body)
                else:
                    print('Response Error:', response.status)
        except aiohttp.ClientError as e:
            print('Connection Error:', e)
    if body:
        return True
    return False
