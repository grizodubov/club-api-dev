import asyncio
from async_pyfcm import AsyncPyFCM



################################################################
def send_push_message(api, recepients_ids, title, message, link):
    if recepients_ids:
        asyncio.create_task(stateful_session(api, recepients_ids, title, message, link))



################################################################
async def stateful_session(api, recepients_ids, title, message, link, data = None):
    #recepients_ids = [ 10004, 10069 ]
    tokens_data = await api.pg.club.fetch(
        """
            SELECT
                device_id, device_token, MAX(time_last_activity) AS time_last_activity
            FROM
                sessions
            WHERE
                user_id = ANY($1) AND device_token <> '' AND device_token IS NOT NULL
            GROUP BY
                device_id, device_token""",
        recepients_ids
    )
    if tokens_data:
        temp = {}
        for item in tokens_data:
            if item['device_id'] not in temp:
                temp[item['device_id']] = dict(item)
            else:
                if temp[item['device_id']]['time_last_activity'] < item['time_last_activity']:
                    temp[item['device_id']] = dict(item)
        tokens = []
        for v in temp.values():
            tokens.append(v['device_token'])
        if tokens:
            m = {
                'notification': {
                    'title': title,
                    'body': message,
                },
                'data': {
                    'link': link
                },
            }
            if data is not None:
                m['data'].update(data)
            async with AsyncPyFCM(google_application_credentials="fcm-credentials.json") as async_fcm:
                responses = await asyncio.gather(
                    *[ task_send(async_fcm, m | { 'token': item }) for item in tokens ]
                )



################################################################
async def task_send(async_fcm, m):
    try:
        response = await async_fcm.send(m)
    except:
        print('ERROR')
    else:
        print('OK')
