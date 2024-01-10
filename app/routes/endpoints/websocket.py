import orjson
from starlette.endpoints import WebSocketEndpoint
from starlette.routing import WebSocketRoute

from app.core.context import set_api_context, get_api_context
from app.models.session import check_by_token



def routes():
    return [
        WebSocketRoute('/ws', Echo)
    ]



class Echo(WebSocketEndpoint):

    encoding = 'text'


    async def on_connect(self, websocket):
        #print('connect! 0')
        api = self.scope['app'].state.api
        set_api_context(api)
        await websocket.accept()
        api.websocket_append(websocket)


    async def on_disconnect(self, websocket, close_code):
        #print('disconnect! 0')
        api = get_api_context()
        #print(api, self.scope['app'].state.api)
        api.websocket_remove(websocket)


    async def on_receive(self, websocket, message):
        #print('message! 0')
        api = get_api_context()
        if message == 'ping':
            try:
                await websocket.send_text('pong')
            except:
                print('websocket error!!!')
            return
        data = orjson.loads(message)
        if 'command' in data:
            if data['command'] == 'register':
                #print('register! 0')
                if 'token' in data and 'user_id' in data:
                    #print('register! 1', data['token'])
                    result = await check_by_token(data['token'])
                    if result and data['user_id'] == result['user_id']:
                        #print('register! 2', result['user_id'])
                        api.websocket_set(websocket, result['user_id'], result['session_id'], data['client'] if 'client' in data else '', data['agent'] if 'agent' in data else '')
