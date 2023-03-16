import os
import uuid
import socket
from functools import partial

from app.core.config import Config
from app.core.redis import RedisBank
from app.core.pg import PgBank
from app.core.mq import MQBank



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
