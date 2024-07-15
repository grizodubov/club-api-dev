import asyncio
import asyncpg
import itertools
from datetime import datetime
import orjson

from app.utils import asyncp
from app.core.logger import ErrorWrapper



####################################################################
def jsonb_encoder(value):
    return b'\x01' + orjson.dumps(value)


####################################################################
def jsonb_decoder(value):
    return orjson.loads(value[1:])


####################################################################
def timestamp_encoder(value):
    if type(value) == int or type(value) == float:
        try:
            return datetime.fromtimestamp(value / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')
        except:
            return datetime.fromtimestamp(value / 1000).strftime('%Y-%m-%d %H:%M:%S')
    else:
        return value


####################################################################
def timestamp_decoder(value):
    try:
        return round(datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f').timestamp() * 1000)
    except:
        return round(datetime.strptime(value, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)


####################################################################
class _ErrorWrapper(ErrorWrapper):


    ERRORS = {
        101: 'Can\'t connect to pg',
        102: 'Pg is too busy',
        103: 'Pg command execution timeout',
        104: 'Pg command failed',
        105: 'Pg check execution timeout',
        106: 'Pg check failed',
        107: 'PgBank reserved word',
        108: 'Pg connection not found',
        109: 'Pg transaction start timeout',
        110: 'Pg transaction start failed',
        111: 'Pg transaction commit timeout',
        112: 'Pg transaction commit failed',
    }

    MESSAGES = {
        201: 'Pg connection established',
    }



####################################################################
class PgBank:


    ################################################################
    def __init__(self, error_helper = None, on_error = None, on_notice = None):
        self.__bank = {}
        self.__helper = error_helper if error_helper else _ErrorWrapper(on_error = on_error, on_notice = on_notice)


    ################################################################
    async def create(self, alias, config):
        if alias in { '__bank', '__helper', 'create', 'drop', 'health', 'status' }:
            self.__helper.err(107, alias)
        else:
            self.__bank[alias] = PgPool(alias, config, error_helper = self.__helper)
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
            if not self.__bank[k].status():
                return False
        return True


    ################################################################
    def __getattr__(self, alias):
        if alias in self.__bank:
            return self.__bank[alias]
        self.__helper.err(108, alias)



####################################################################
class PgPool:


    ################################################################
    def __init__(self, alias, config, error_helper = None, on_error = None, on_notice = None):
        self.alias = alias
        self.config = config
        self.helper = error_helper if error_helper else _ErrorWrapper(on_error = on_error, on_notice = on_notice)
        self.pool = []
        self.semaphore = asyncp.Semaphore(
            self.config['MAX_CONNECTIONS_PER_WORKER'],
            timeout = self.config['QUEUE_TIMEOUT']
        )
        self.init()


    ################################################################
    def init(self):
        for _ in itertools.repeat(None, self.config['MIN_ACTIVE_CONNECTIONS_PER_WORKER']):
            self.pool.append(
                [ Pg(self.alias, self.config, error_helper = self.helper), True ]
            )


    ################################################################
    async def connect(self):
        await asyncio.gather(*[ conn[0].connect() for conn in self.pool ])


    ################################################################
    async def acquire(self):
        conn = await self._take()
        await conn.acquire(on_finish = self._release)
        return conn


    ################################################################
    async def release(self, connection, rollback = False):
        await connection.release(rollback)


    ################################################################
    async def fetch(self, *args, **kwargs):
        conn = await self._take()
        result = await conn.fetch(*args, **kwargs)
        self._release(conn)
        return result

    
    ################################################################
    async def fetchrow(self, *args, **kwargs):
        conn = await self._take()
        result = await conn.fetchrow(*args, **kwargs)
        self._release(conn)
        return result


    ################################################################
    async def fetchval(self, *args, **kwargs):
        conn = await self._take()
        result = await conn.fetchval(*args, **kwargs)
        self._release(conn)
        return result



    ################################################################
    async def execute(self, *args, **kwargs):
        conn = await self._take()
        await conn.execute(*args, **kwargs)
        self._release(conn)


    ################################################################
    def health(self):
        return {
            'max_connections': self.config['MAX_CONNECTIONS_PER_WORKER'],
            'pool': [ conn[0].health() for conn in self.pool ],
        }


    ################################################################
    def status(self):
        for conn in self.pool:
            if not conn[0].status:
                return False
        return True
    

    ################################################################
    async def _take(self):
        try:
            await self.semaphore.acquire()
        except asyncp.LockTimeoutError:
            self.helper.err(102, self.alias)
            return None
        for conn in self.pool:
            if conn[1]:
                conn[1] = False
                return conn[0]
        conn = [ Pg(self.alias, self.config, error_helper = self.helper), False ]
        await conn[0].connect()
        self.pool.append(conn)
        return conn[0]


    ################################################################
    def _release(self, connection):
        for conn in self.pool:
            if connection == conn[0]:
                conn[1] = True
                self.semaphore.release()



####################################################################
class Pg:


    ################################################################
    def __init__(self, alias, config, error_helper = None, on_error = None, on_notice = None):
        self.alias = alias
        self.config = config
        self.helper = error_helper if error_helper else _ErrorWrapper(on_error = on_error, on_notice = on_notice)
        self.lock = asyncp.Lock(timeout = self.config['QUEUE_TIMEOUT'])
        self.handler = None
        self.transaction = None
        self.transaction_destructor = None
        self.on_transaction_start = None
        self.on_transaction_finish = None
        self.status = False
        self.inspector = None
        self.latency = 0
        self.start = 0
        self.action = 0


    ################################################################
    async def connect(self):
        credentials = {
            'host': self.config['HOST'],
            'port': self.config['PORT'],
            'database': self.config['DATABASE'],
            'user': self.config['USER'],
            'password': self.config['PASSWORD'],
            'ssl': 'require',
        }
        try:
            self.handler = await asyncpg.connect(**credentials)
        except Exception as e:
            self.status = False
            self.start = 0
            self.helper.err(101, self.alias, str(e))
        else:
            await self.handler.set_type_codec(
                'jsonb',
                encoder = jsonb_encoder,
                decoder = jsonb_decoder,
                schema = 'pg_catalog',
                format = 'binary',
            )
            await self.handler.set_type_codec(
                'timestamp',
                encoder = timestamp_encoder,
                decoder = timestamp_decoder,
                schema = 'pg_catalog',
            )
            self.status = True
            self.start = datetime.now()
            self.helper.msg(201, self.alias)
        self._run_inspector()


    ################################################################
    async def check(self):
        start = datetime.now()
        self.status = await self.ping()
        if self.status:
            finish = datetime.now()
            self.latency = (finish - start).total_seconds()
        else:
            self.latency = 0


    ################################################################
    async def ping(self):
        try:
            result = await self.lock.execute(
                self.handler.fetchval('SELECT 1'),
                timeout = self.config['COMMAND_TIMEOUT']
            )
        except asyncp.LockTimeoutError:
            self.helper.err(102, self.alias)
        except asyncp.ExecutionTimeoutError:
            self.helper.err(105, self.alias)
        except asyncp.ExecutionError:
            self.helper.err(106, self.alias)
        else:
            if result == 1:
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
        self.start = 0
        self.action = 0
        self.idle = False
        self.lock.release()


    ################################################################
    async def acquire(self, on_start = None, on_finish = None):
        try:
            await self.lock.acquire()
        except asyncp.LockTimeoutError:
            self.helper.err(102, self.alias)
            return None
        try:
            tr = await asyncp.execute(
                self.start_transaction(),
                timeout = self.config['COMMAND_TIMEOUT']
            )
        except asyncp.ExecutionTimeoutError:
            self.transaction = None
            self.helper.err(109, self.alias)
        except asyncp.ExecutionError:
            self.transaction = None
            self.helper.err(110, self.alias)
        else:
            loop = asyncio.get_event_loop()
            self.transaction_destructor = loop.create_task(self._drop_transaction())
            self.on_transaction_start = on_start
            self.on_transaction_finish = on_finish
            if self.on_transaction_start:
                self.on_transaction_start(self)
                self.on_transaction_start = None


    ################################################################
    async def release(self, rollback = False):
        try:
            tr = await asyncp.execute(
                self.finish_transaction(rollback),
                timeout = self.config['COMMAND_TIMEOUT']
            )
        except asyncp.ExecutionTimeoutError:
            self.helper.err(111, self.alias)
        except asyncp.ExecutionError:
            self.helper.err(112, self.alias)
        self.lock.release()
        if self.on_transaction_finish:
            self.on_transaction_finish(self)
            self.on_transaction_finish = None


    ################################################################
    async def start_transaction(self):
        self.transaction = self.handler.transaction()
        await self.transaction.start()


    ################################################################
    async def finish_transaction(self, rollback = False):
        if rollback:
            await self.transaction.rollback()
        else:
            await self.transaction.commit()
        if self.transaction_destructor:
            self.transaction_destructor.cancel()
            self.transaction_destructor = None
        self.transaction = None


    ################################################################
    async def fetch(self, *args, **kwargs):
        func = asyncp.execute if self.transaction else self.lock.execute
        return await self._fetch(func, self.handler.fetch, *args, **kwargs)


    ################################################################
    async def fetchrow(self, *args, **kwargs):
        func = asyncp.execute if self.transaction else self.lock.execute
        return await self._fetch(func, self.handler.fetchrow, *args, **kwargs)


    ################################################################
    async def fetchval(self, *args, **kwargs):
        func = asyncp.execute if self.transaction else self.lock.execute
        return await self._fetch(func, self.handler.fetchval, *args, **kwargs)


    ################################################################
    async def execute(self, *args, **kwargs):
        func = asyncp.execute if self.transaction else self.lock.execute
        return await self._execute(func, self.handler.execute, *args, **kwargs)


    ################################################################
    def status(self):
        return self.status if self.handler else None


    ################################################################
    def health(self):
        n = datetime.now()
        report = {
            'status': self.status,
            'latency': self.latency,
            'live': 0,
            'inactive': 0,
        }
        if self.start:
            report['live'] = (n - self.start).total_seconds()
            if self.action:
                report['inactive'] = (n - self.action).total_seconds()
            else:
                report['inactive'] = report['live']
        return report


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
    async def _drop_transaction(self, rollback = False):
        await asyncio.sleep(self.config['MAX_TRANSACTION_TIME'])
        await self.finish_transaction(rollback = True)


    ################################################################
    async def _fetch(self, wrapper, command, *args, **kwargs):
        try:
            result = await wrapper(
                command(*args, **kwargs),
                timeout = self.config['COMMAND_TIMEOUT']
            )
        except asyncp.LockTimeoutError:
            self.helper.err(102, self.alias)
        except asyncp.ExecutionTimeoutError:
            self.helper.err(103, self.alias)
        except asyncp.ExecutionError:
            self.helper.err(104, self.alias, ', '.join(map(str, args)))
        else:
            self.action = datetime.now()
            return result
        return None


    ################################################################
    async def _execute(self, wrapper, command, *args, **kwargs):
        try:
            await wrapper(
                command(*args, **kwargs),
                timeout = self.config['COMMAND_TIMEOUT']
            )
        except asyncp.LockTimeoutError:
            self.helper.err(102, self.alias)
        except asyncp.ExecutionTimeoutError:
            self.helper.err(103, self.alias)
        except asyncp.ExecutionError:
            self.helper.err(104, self.alias, ', '.join(map(str, args)))
        else:
            self.action = datetime.now()
