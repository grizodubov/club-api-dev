import asyncio
from redis import asyncio as aioredis
import gzip
import datetime

from app.utils import asyncp
from app.utils.packager import pack as data_pack, unpack as data_unpack
from app.core.logger import ErrorWrapper



####################################################################
class _ErrorWrapper(ErrorWrapper):


    ERRORS = {
        101: 'Can\'t connect to redis',
        102: 'Redis is too busy',
        103: 'Redis command execution timeout',
        104: 'Redis command failed',
        105: 'Redis ping execution timeout',
        106: 'Redis ping failed',
        107: 'RedisBank reserved word',
        108: 'Redis connection not found',
    }

    MESSAGES = {
        201: 'Redis connection established',
    }



####################################################################
class RedisBank:


    ################################################################
    def __init__(self, error_helper = None, on_error = None, on_notice = None):
        self.__bank = {}
        self.__helper = error_helper if error_helper else _ErrorWrapper(on_error = on_error, on_notice = on_notice)


    ################################################################
    async def create(self, alias, config):
        if alias in { '__bank', '__helper', 'create', 'drop', 'health', 'status' }:
            self.__helper.err(107, alias)
        else:
            self.__bank[alias] = Redis(alias, config, error_helper = self.__helper)
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



####################################################################
class Redis:


    ################################################################
    def __init__(self, alias, config, error_helper = None, on_error = None, on_notice = None):
        self.alias = alias
        self.config = config
        self.helper = error_helper if error_helper else _ErrorWrapper(on_error = on_error, on_notice = on_notice)
        self.lock = asyncp.Lock(timeout = self.config['QUEUE_TIMEOUT'])
        self.handler = None
        self.block = False
        self.status = False
        self.inspector = None
        self.latency = 0


    ################################################################
    async def connect(self):
        credentials = {
            'host': self.config['HOST'],
            'port': self.config['PORT'],
            'db': self.config['DATABASE'],
        }
        if self.config['PASSWORD']:
            credentials['password'] = self.config['PASSWORD']
        try:
            self.handler = await aioredis.Redis(**credentials)
        except aioredis.RedisError as e:
            self.status = False
            self.helper.err(101, self.alias, str(e))
        else:
            self.status = True
            self.helper.msg(201, self.alias)
        self._run_inspector()


    ################################################################
    async def check(self):
        start = datetime.datetime.now()
        self.status = await self.ping()
        if self.status:
            finish = datetime.datetime.now()
            self.latency = (finish - start).total_seconds()
        else:
            self.latency = 0


    ################################################################
    async def ping(self):
        try:
            result = await self.lock.execute(
                self.handler.ping(),
                #self.handler.execute('PING'),
                timeout = self.config['COMMAND_TIMEOUT']
            )
        except asyncp.LockTimeoutError:
            self.helper.err(102, self.alias)
        except asyncp.ExecutionTimeoutError:
            self.helper.err(105, self.alias)
        except asyncp.ExecutionError:
            self.helper.err(106, self.alias)
        else:
            if result is True:
                return True
            else:
                self.helper.err(106, self.alias)
        return False


    ################################################################
    async def disconnect(self):
        await self.lock.acquire()
        if self.inspector is not None:
            self.inspector.cancel()
            self.inspector = None
        if self.handler:
            await self.handler.close()
            self.handler = None
        self.status = False
        self.latency = 0
        self.lock.release()


    ################################################################
    async def acquire(self):
        try:
            await self.lock.acquire()
        except asyncp.LockTimeoutError:
            self.helper.err(102, self.alias)
        else:
            self.block = True


    ################################################################
    def release(self):
        self.block = False
        self.lock.release()


    ################################################################
    async def get(self, key):
        func = asyncp.execute if self.block else self.lock.execute
        try:
            data = await func(
                self.handler.get(key),
                #self.handler.execute(b'GET', key.encode()),
                timeout = self.config['COMMAND_TIMEOUT']
            )
        except asyncp.LockTimeoutError:
            self.helper.err(102, self.alias)
        except asyncp.ExecutionTimeoutError:
            self.helper.err(103, self.alias)
        except asyncp.ExecutionError:
            self.helper.err(104, self.alias)
        else:
            return data_unpack(data)
        return None


    ################################################################
    async def set(self, key, value, *args):
        data = data_pack(value, False)
        return await self._set(key, data, *args)


    ################################################################
    async def zset(self, key, value, *args):
        data = data_pack(value, True)
        return await self._set(key, data, *args)


    ################################################################
    async def exec(self, command, *args):
        func = asyncp.execute if self.block else self.lock.execute
        params = [ arg.encode() if type(arg).__name__ == 'str' else arg for arg in args ]
        try:
            ex = gettatr(self.handler, command.lower())
            data = await func(
                ex(*params),
                timeout = self.config['COMMAND_TIMEOUT']
            )
        except asyncp.LockTimeoutError:
            self.helper.err(102, self.alias)
        except asyncp.ExecutionTimeoutError:
            self.helper.err(103, self.alias)
        except asyncp.ExecutionError:
            self.helper.err(104, self.alias)
        else:
            return data
        return None


    ################################################################
    def health(self):
        return { k: getattr(self, k) for k in { 'status', 'latency' } }


    ################################################################
    def status(self):
        return self.status if self.handler else None


    ################################################################
    def _run_inspector(self):
        if self.inspector is None:
            loop = asyncio.get_event_loop()
            self.inspector = loop.create_task(self._inspect())


    ################################################################
    async def _inspect(self):
        while True:
            await self.check()
            await asyncio.sleep(self.config['PING_INTERVAL'])


    ################################################################
    async def _set(self, key, data, *args):
        func = asyncp.execute if self.block else self.lock.execute
        params = [ arg.encode() if type(arg).__name__ == 'str' else arg for arg in args ]
        try:
            result = await func(
                self.handler.set(key, data, *params),
                #self.handler.execute(b'SET', key.encode(), data, *params),
                timeout = self.config['COMMAND_TIMEOUT']
            )
        except asyncp.LockTimeoutError:
            self.helper.err(102, self.alias)
        except asyncp.ExecutionTimeoutError:
            self.helper.err(103, self.alias)
        except asyncp.ExecutionError:
            self.helper.err(104, self.alias)
        else:
            return result.decode()
        return None
