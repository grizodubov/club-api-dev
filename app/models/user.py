import re

from app.core.context import get_api_context



####################################################################
class User:


    ################################################################
    def __init__(self):
        self.id = 0
        self.time_create = None
        self.time_update = None
        self.name = ''
        self.login = ''
        self.email = ''
        self.phone = ''
        self.company = ''
        self.position = ''
        self.description = ''
        self.status = ''
        self.tags = ''
        self._password = ''


    ################################################################
    async def set(self, id):
        api = get_api_context()
        if id:
            data = await api.pg.club.fetchrow(
                """SELECT
                        t1.id, t1.time_create, t1.time_update,
                        t1.name, t1.login, t1.email, t1.phone,
                        t3.company, t3.position, t3.description,
                        t3.status, t2.tags, 
                        t1.password AS _password
                    FROM
                        users t1
                    INNER JOIN
                        users_tags t2 ON t2.user_id = t1.id
                    INNER JOIN
                        users_info t3 ON t3.user_id = t1.id
                    WHERE
                        id = $1 AND
                        active IS TRUE""",
                id
            )
            self.__dict__ = { **self.__dict__, **dict(data) }


    ################################################################
    async def copy(self, user):
        self.__dict__ = user.__dict__.copy()


    ################################################################
    async def find(self, **kwargs):
        api = get_api_context()
        if kwargs:
            check = re.compile(r'[a-z_\d]+')
            qr = []
            ar = []
            i = 1
            for k, v in kwargs.items():
                if re.fullmatch(check, k) is not None:
                    qr.append(k + ' = $' + str(i))
                    if k == 'phone':
                        # Только мобильные телефоны РФ
                        ar.append('+7' + ''.join(list(re.sub(r'[^\d]+', '', v))[-10:]))
                    else:
                        ar.append(v)
                    i += 1
            if qr:
                data = await api.pg.club.fetchrow(
                    """SELECT
                            t1.id, t1.time_create, t1.time_update,
                            t1.name, t1.login, t1.email, t1.phone,
                            t3.company, t3.position, t3.description,
                            t3.status, t2.tags, 
                            t1.password AS _password
                        FROM
                            users t1
                        INNER JOIN
                            users_tags t2 ON t2.user_id = t1.id
                        INNER JOIN
                            users_info t3 ON t3.user_id = t1.id
                        WHERE """ + ' AND '.join(qr),
                    *ar
                )
                if data:
                    self.__dict__ = { **self.__dict__, **dict(data) }
                    return True
        return False


    ################################################################
    async def check(self, account, password):
        api = get_api_context()
        if account and password:
            # Только мобильные телефоны РФ
            phone = '+7' + ''.join(list(re.sub(r'[^\d]+', '', account))[-10:])
            data = await api.pg.club.fetchrow(
                """SELECT
                        t1.id, t1.time_create, t1.time_update,
                        t1.name, t1.login, t1.email, t1.phone,
                        t3.company, t3.position, t3.description,
                        t3.status, t2.tags, 
                        t1.password AS _password
                    FROM
                        users t1
                    INNER JOIN
                        users_tags t2 ON t2.user_id = t1.id
                    INNER JOIN
                        users_info t3 ON t3.user_id = t1.id
                    WHERE
                        (login = $1 OR email = $1 OR phone = $2) AND
                        active IS TRUE""",
                account, phone
            )
            if data and data['_password'] == password:
                self.__dict__ = { **self.__dict__, **dict(data) }
                return True
        return False



    ################################################################
    async def set_validation_code(self, code):
        api = get_api_context()
        await api.redis.data.exec('SET', '_AUTH_' + str(self.id) + '_' + code, 1, ex = 300)
