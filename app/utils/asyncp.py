import asyncio



####################################################################
class Error(Exception):
    pass

####################################################################
class LockTimeoutError(Error):
    pass

####################################################################
class ExecutionTimeoutError(Error):
    pass

####################################################################
class ExecutionError(Error):
    pass



####################################################################
async def execute(coro, timeout):
    try:
        result = await asyncio.wait_for(
            coro,
            timeout = timeout
        )
    except asyncio.TimeoutError:
        raise ExecutionTimeoutError
    except Exception as e:
        #TODO: remove e and print
        print(e)
        raise ExecutionError
    return result



####################################################################
class Lock:


    ################################################################
    def __init__(self, timeout):
        self.lock = asyncio.Lock()
        self.acquire_timeout = timeout


    ################################################################
    async def acquire(self):
        try:
            await asyncio.wait_for(
                self.lock.acquire(),
                timeout = self.acquire_timeout
            )
        except asyncio.TimeoutError:
            raise LockTimeoutError


    ################################################################
    def release(self):
        self.lock.release()


    ################################################################
    async def execute(self, coro, timeout):
        await self.acquire()   
        try:
            result = await asyncio.wait_for(
                coro,
                timeout = timeout
            )
        except asyncio.TimeoutError:
            self.release()
            raise ExecutionTimeoutError
        except Exception as e:
            self.release()
            #TODO: remove e and print
            print(e)
            raise ExecutionError
        else:
            self.release()
        return result


    ################################################################
    def locked(self):
        return self.lock.locked()



####################################################################
class Semaphore(Lock):


    ################################################################
    def __init__(self, amount, timeout):
        self.lock = asyncio.Semaphore(amount)
        self.acquire_timeout = timeout
