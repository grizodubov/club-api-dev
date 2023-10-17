import os
import uuid
import socket
import asyncio
import orjson
from functools import partial

from app.core.config import Config
from app.core.redis import RedisBank
from app.core.pg import PgBank
from app.core.mq import MQBank

from app.core.stream import Stream



####################################################################
class API:


    ################################################################
    def __init__(self):
        self.pid = os.getpid()
        self.host = socket.gethostname()
        self.uid = '{}-{}'.format(str(uuid.uuid1()), str(self.pid))
        self.config = Config()
        self.redis = RedisBank(
            on_notice = partial(self.log, 'Notice'),
            on_error = self.err,
        )
        self.mq = MQBank(
            on_notice = partial(self.log, 'Notice'),
            on_error = self.err,
        )
        self.pg = PgBank(
            on_notice = partial(self.log, 'Notice'),
            on_error = self.err,
        )
        self.store = {
            'websockets': [],
        }
        self.stream_email = Stream('mail', timeout = 1, retry_error = True, timeout_error = 5)
        self.stream_mobile = Stream('mobile', timeout = 1, retry_error = True, timeout_error = 5)


    ################################################################
    async def init(self):
        # redis
        for db in self.config.settings['API']['REDIS']:
            await self.redis.create(db, self.config.settings['REDIS'] | self.config.redis[db])
        # mq
        for db in self.config.settings['API']['MQ']:
            await self.mq.create(db, self.config.settings['REDIS'] | self.config.redis[db])
        # pg
        for db in self.config.settings['API']['PG']:
            await self.pg.create(db, self.config.settings['PG'] | self.config.pg[db])
        self.log('Notice', 'Worker ' + self.uid + ' started with pid ' + str(self.pid))


    ################################################################
    def err(self, detail):
        self.log('Error', detail)


    ################################################################
    def log(self, type, detail):
        print(f'{type}: {detail}')


    ################################################################
    def websocket_append(self, websocket, user_id = 0, session_id = 0):
        self.store['websockets'].append({
            'handler': websocket,
            'user_id': user_id,
            'session_id': session_id,
        })
        if user_id:
            self.websocket_mass_send({ 'auth': True, 'status': True, 'user_id': user_id })


    ################################################################
    def websocket_set(self, websocket, user_id, session_id):
        for ws in self.store['websockets']:
            if ws['handler'] == websocket:
                ws['user_id'] = user_id
                ws['session_id'] = session_id
                if user_id:
                    self.websocket_mass_send({ 'auth': True, 'status': True, 'user_id': user_id })


    ################################################################
    def websocket_update(self, session_id, user_id):
        for ws in self.store['websockets']:
            if ws['session_id'] == session_id:
                if ws['user_id']:
                    self.websocket_mass_send({ 'auth': True, 'status': False, 'user_id': ws['user_id'] })
                ws['user_id'] = user_id
                if user_id:
                    self.websocket_mass_send({ 'auth': True, 'status': True, 'user_id': user_id })


    ################################################################
    def websocket_remove(self, websocket):
        temp = []
        users = []
        for index, ws in enumerate(self.store['websockets']):
            if ws['handler'] == websocket:
                temp.append(index)
                if ws['user_id']:
                    users.append(ws['user_id'])
        for index in reversed(temp):
            self.store['websockets'].pop(index)
        if users:
            for user_id in set(users):
                self.websocket_mass_send({ 'auth': True, 'status': False, 'user_id': user_id })



    ################################################################
    async def websocket_send(self, user_id, message):
        for ws in self.store['websockets']:
            if ws['user_id'] == user_id:
                await ws['handler'].send_text(message)



    ################################################################
    def websocket_mass_send(self, message):
        for ws in self.store['websockets']:
            asyncio.create_task(ws['handler'].send_text(orjson.dumps(message).decode()))



     ################################################################
    def websocket_limited_send(self, users_ids, message):
        for ws in self.store['websockets']:
            if ws['user_id'] in users_ids:
                asyncio.create_task(ws['handler'].send_text(orjson.dumps(message).decode()))


 
    ################################################################
    def websocket_remove(self, websocket):
        temp = []
        users = []
        for index, ws in enumerate(self.store['websockets']):
            if ws['handler'] == websocket:
                temp.append(index)
                if ws['user_id']:
                    users.append(ws['user_id'])
        for index in reversed(temp):
            self.store['websockets'].pop(index)
        if users:
            for user_id in set(users):
                self.websocket_mass_send({ 'auth': True, 'status': False, 'user_id': user_id })


    ################################################################
    def users_online(self):
        users = []
        for index, ws in enumerate(self.store['websockets']):
            if ws['user_id']:
                users.append(ws['user_id'])
        # print('ONLINE', users)
        return set(users)
