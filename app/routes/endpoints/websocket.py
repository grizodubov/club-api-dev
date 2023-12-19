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
        api = self.scope['app'].state.api
        set_api_context(api)
        await websocket.accept()
        api.websocket_append(websocket)


    async def on_disconnect(self, websocket, close_code):
        api = self.scope['app'].state.api
        print('disconnect! 0')
        #api = get_api_context()
        #print(api, self.scope['app'].state.api)
        api.websocket_remove(websocket)


    async def on_receive(self, websocket, message):
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
                if 'token' in data and 'user_id' in data:
                    result = await check_by_token(data['token'])
                    if result and data['user_id'] == result['user_id']:
                        api.websocket_set(websocket, result['user_id'], result['session_id'])
