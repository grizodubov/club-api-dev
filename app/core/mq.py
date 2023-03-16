import asyncio

from app.utils.packager import unpack as data_unpack
from app.core.redis import _ErrorWrapper, RedisBank, Redis



####################################################################
class MQBank():


    ################################################################
    def __init__(self, error_helper = None, on_error = None, on_notice = None):
        self.__bank = {}
        self.__helper = error_helper if error_helper else _ErrorWrapper(on_error = on_error, on_notice = on_notice)


    ################################################################
    async def create(self, alias, config):
        if alias in { '__bank', '__helper', 'create', 'drop', 'health', 'status' }:
            self.__helper.err(107, alias)
        else:
            self.__bank[alias] = MQRedis(alias, config, error_helper = self.__helper)
            await self.__bank[alias].connect()


    ################################################################
    async def drop(self, alias):
        if alias in self.__bank:
            await self.__bank[alias].disconnect()
            del self.__bank[alias]


    ################################################################
    def health(self, alias = None):
        return { k: self.__bank[k].health() for k in self.__bank }


    ################################################################
    def status(self, alias = None):
        for k in self.__bank:
            if not self.__bank[k].status:
                return False
        return True


    ################################################################
    def __getattr__(self, alias):
        if alias in self.__bank:
            return self.__bank[alias]
        self.__helper.err(108, alias)



###################################################################
class MQRedis(Redis):


    ################################################################
    def __init__(self, alias, config, error_helper = None, on_error = None, on_notice = None):
        self.channels = {}
        super(MQRedis, self).__init__(alias, config, error_helper = error_helper, on_error = on_error, on_notice = on_notice)


    ################################################################
    def subscribe(self, channel, on_message, *args):
        self.channels[channel] = MQSubscription(self, channel, on_message, *args)


    ################################################################
    def unsubscribe(self, channel):
        self.channels[channel].cancel()
        del self.channels[channel]


    ################################################################
    async def publish(self, channel, message_id, message_data):
        key = '__MQ_MSG__' + channel + '__' + str(message_id).zfill(20)
        await self.set(key, message_data, 'EX', str(self.config['MQ_MESSAGE_LIFETIME']))



####################################################################
class MQSubscription:


    ################################################################
    def __init__(self, redis, channel, on_message, *args):
        self.redis = redis
        self.channel = channel
        self.channel_mask = '__MQ_MSG__' + channel + '__*'
        self.on_message = on_message
        self.args = args
        self.listener = None
        self.state = set()
        self.loop = asyncio.get_event_loop()
        self.init()


    ################################################################
    def init(self):
        self.inspector = self.loop.create_task(self.check())


    ################################################################
    def cancel(self):
        if self.listener is not None:
            self.listener.cancel()
            self.listener = None


    ################################################################
    async def check(self):
        while True:
            result = set()
            cur = None
            conn = await self.redis.acquire()
            while cur != b'0':
                cur, keys = await self.redis.exec('SCAN', cur if cur else b'0', 'MATCH', self.channel_mask)
                if keys:
                    result.update(keys)
            self.redis.release()
            keys_to_process = result - self.state
            self.state = result
            if keys_to_process:
                self.loop.create_task(self.process(keys_to_process))
            await asyncio.sleep(self.redis.config['MQ_CHECK_INTERVAL'])


    ################################################################
    async def process(self, keys):
        events = await self.redis.exec('MGET', *keys)
        for event in events:
            self.loop.create_task(self.on_message(data_unpack(event), *self.args))
