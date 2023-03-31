import aiohttp



################################################################
async def send_mobile_message(recepient, message):
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
                    'target': recepient,
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
