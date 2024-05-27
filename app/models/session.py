import re
from secrets import token_hex

from app.core.context import get_api_context



####################################################################
class Session:


    ################################################################
    def __init__(self):
        self.id = 0
        self.key = ''
        self.token_prev = ''
        self.token_next = ''
        self.user_id = 0
        self.time_last_activity = ''
        self.settings = {}


    ################################################################
    async def auth_by_token(self, token = ''):
        api = get_api_context()
        id = 0
        data = None
        if re.fullmatch(r'[0-9A-Fa-f]{64}', token):
            session_id = await api.redis.tokens.exec('GET', token)
            if session_id:
                id = int(session_id)
                if id:
                    data = await api.pg.club.fetchrow(
                        """UPDATE
                                sessions
                            SET
                                time_last_activity = now() at time zone 'utc'
                            WHERE
                                id = $1
                            RETURNING
                                coalesce(user_id, 0) AS user_id,
                                time_last_activity,
                                settings""",
                        id
                    )
        if data:
            self.id = id
            self.token_prev = token
            self.user_id = data['user_id']
            await api.redis.tokens.exec('EXPIRE', token, api.config.settings['AUTH']['TOKEN_DROPTIME'])
        else:
            data = await api.pg.club.fetchrow(
                """INSERT INTO
                        sessions
                    DEFAULT VALUES
                    RETURNING
                        id,
                        time_last_activity,
                        settings"""
            )
            self.id = data['id']
            self.token_prev = ''
            self.user_id = 0
        self.time_last_activity = data['time_last_activity']
        self.settings = data['settings']
        await api.redis.tokens.acquire()
        while True:
            token_next = token_hex(32)
            result = await api.redis.tokens.exec('EXISTS', token_next)
            if result == 0:
                break
        self.token_next = token_next
        await api.redis.tokens.exec('SET', token_next, self.id, ex = api.config.settings['AUTH']['TOKEN_LIFETIME'])
        api.redis.tokens.release()


    ################################################################
    async def auth_by_key(self, key = ''):
        api = get_api_context()
        if re.fullmatch(r'[0-9A-Za-z]{128}', key):
            data = await api.pg.club.fetchrow(
                """UPDATE
                        keys
                    SET
                        time_last_activity = now() at time zone 'utc'
                    WHERE
                        key = $1
                    RETURNING
                        coalesce(user_id, 0) AS user_id,
                        time_last_activity,
                        settings""",
                key
            )
            if data:
                self.user_id = data['user_id']
                self.key = key
                self.time_seen = data['time_last_activity']
                self.settings = data['settings']
    

    ################################################################
    async def auth_by_subtoken(self, token = '', subtoken = ''):
        api = get_api_context()
        id = 0
        data = None
        user_id = 0
        if re.fullmatch(r'[0-9A-Fa-f]{64}', subtoken):
            user_id = await api.redis.tokens.exec('GET', subtoken)
            if not user_id or user_id == '0':
                user_id = 0
            else:
                user_id = int(user_id)
        if re.fullmatch(r'[0-9A-Fa-f]{64}', token):
            session_id = await api.redis.tokens.exec('GET', token)
            if session_id:
                id = int(session_id)
                if id:
                    data = await api.pg.club.fetchrow(
                        """UPDATE
                                sessions
                            SET
                                time_last_activity = now() at time zone 'utc'
                            WHERE
                                id = $1
                            RETURNING
                                coalesce(user_id, 0) AS user_id,
                                time_last_activity,
                                settings""",
                        id
                    )
        if data:
            self.id = id
            self.token_prev = token
            if not user_id or user_id == data['user_id']:
                self.user_id = data['user_id']
            else:
                if data['user_id']:
                    self.user_id = 0
                    await api.pg.club.fetchrow(
                        """UPDATE
                                sessions
                            SET
                                user_id = NULL
                            WHERE
                                id = $1""",
                        self.id
                    )
                else:
                    self.user_id = user_id
                    await api.pg.club.fetchrow(
                        """UPDATE
                                sessions
                            SET
                                user_id = $2
                            WHERE
                                id = $1""",
                        self.id, user_id
                    )

            await api.redis.tokens.exec('EXPIRE', token, api.config.settings['AUTH']['TOKEN_DROPTIME'])
        else:
            data = await api.pg.club.fetchrow(
                """INSERT INTO
                        sessions
                    DEFAULT VALUES
                    RETURNING
                        id,
                        time_last_activity,
                        settings"""
            )
            self.id = data['id']
            self.token_prev = ''
            if user_id:
                self.user_id = user_id
                await api.pg.club.fetchrow(
                    """UPDATE
                            sessions
                        SET
                            user_id = $2
                        WHERE
                            id = $1""",
                    self.id, user_id
                )
        self.time_last_activity = data['time_last_activity']
        self.settings = data['settings']
        await api.redis.tokens.acquire()
        while True:
            token_next = token_hex(32)
            result = await api.redis.tokens.exec('EXISTS', token_next)
            if result == 0:
                break
        self.token_next = token_next
        await api.redis.tokens.exec('SET', token_next, self.id, ex = api.config.settings['AUTH']['TOKEN_LIFETIME'])
        api.redis.tokens.release()


    ################################################################
    async def assign(self, user_id):
        api = get_api_context()
        await api.pg.club.execute(
            """UPDATE
                    sessions
                SET
                    user_id = $1
                WHERE
                    id = $2""",
            user_id if user_id else None, self.id
        )
        self.user_id = user_id


    ################################################################
    async def register_device(self, device_id, device_info, device_token):
        api = get_api_context()
        await api.pg.club.execute(
            """UPDATE
                    sessions
                SET
                   device_id = $2,
                   device_info = $3,
                   device_token = $4 
                WHERE
                    id = $1""",
            self.id, device_id, device_info, device_token
        )



####################################################################
async def check_by_token(token):
    api = get_api_context()
    if re.fullmatch(r'[0-9A-Fa-f]{64}', token):
        id = await api.redis.tokens.exec('GET', token)
        if id:
            session_id = int(id)
            data = await api.pg.club.fetchrow(
                """SELECT
                        id AS session_id, coalesce(user_id, 0) AS user_id
                    FROM
                        sessions
                    WHERE
                        id = $1""",
                session_id
            )
            if data:
                return {
                    'session_id': data['session_id'],
                    'user_id': data['user_id'],
                }
    return None
