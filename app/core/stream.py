import re
import asyncio
import functools
from collections import deque


####################################################################
class Stream:


    ################################################################
    def __init__(self, alias, timeout = None, retry_error = True, timeout_error = 1, process_callback = None):
        self.alias = alias
        self.pool = deque([])
        self.loop = asyncio.get_event_loop()
        self.flag = asyncio.Event()
        self.timeout = timeout
        self.retry_error = retry_error
        self.timeout_error = timeout_error
        self.process_callback = process_callback
        self.loop.create_task(self.execute())


    ################################################################
    def register(self, call, *args, **kwargs):
        self.pool.append(functools.partial(call, *args, **kwargs))
        self.flag.set()


    ################################################################
    async def execute(self):
        print('STREAM STARTING', self.alias)
        while True:
            try:
                call = self.pool.popleft()
            except:
                print('STREAM WAITING', self.alias)
                self.flag.clear()
                await self.flag.wait()
            else:
                try:
                    print('STREAM CALL', self.alias)
                    await call()
                except Exception as e:
                    msg = str(e)
                    print(msg)
                    if msg.startswith('Telegram link error'):
                        chat_id = re.sub(r'[^\d]+', '', msg)
                        if chat_id and self.process_callback:
                            print(chat_id, self.process_callback)
                            self.process_callback(chat_id)
                    else:
                        if self.retry_error:
                            if self.timeout_error:
                                self.pool.appendleft(call)
                                await asyncio.sleep(self.timeout_error)
                else:
                    if self.timeout:
                        await asyncio.sleep(self.timeout)
