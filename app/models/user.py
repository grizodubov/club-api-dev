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
    @classmethod
    async def search(cls, text):
        api = get_api_context()
        
                data = await api.pg.club.fetch(
                    """SELECT
                            t1.id, t1.time_create, t1.time_update,
                            t1.name, t1.login, t1.email, t1.phone,
                            t3.company, t3.position, t3.description,
                            t3.status, coalesce(t2.tags, '') AS tags,
                            t1.password AS _password
                        FROM
                            users t1
                        INNER JOIN
                            users_tags t2 ON t2.user_id = t1.id
                        INNER JOIN
                            users_info t3 ON t3.user_id = t1.id
                        WHERE
                            t1.name like '%$1%' OR
                            t1.email like '%$1%' OR
                            t1.phone like '%$1%' OR
                            t3.company like '%$1%' OR
                            t3.position like '%$1%' OR
                            t3.description like '%$1%' OR
                            to_tsvector(t2.tags) @@ to_tsquery($2)""",
                    text, re.sub(r'\s+', ' | ', text)
                )
                for row in data:
                    item = User()
                    item.__dict__ = dict(row)
                    result.append(item)
        return result
    
    
    ################################################################
    async def show(self, id):
        return { k: v for k, v in self.__dict__.items() if not k.startswith('_') }


    ################################################################
    async def set(self, id):
        api = get_api_context()
        if id:
            data = await api.pg.club.fetchrow(
                """SELECT
                        t1.id, t1.time_create, t1.time_update,
                        t1.name, t1.login, t1.email, t1.phone,
                        t3.company, t3.position, t3.description,
                        t3.status, coalesce(t2.tags, '') AS tags,
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
            self.__dict__ = dict(data)


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
                            t3.status, coalesce(t2.tags, '') AS tags,
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
                    self.__dict__ = dict(data)
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
                        t3.status, coalesce(t2.tags, '') AS tags,
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
                self.__dict__ = dict(data)
                return True
        return False


    ################################################################
    async def set_validation_code(self, code):
        api = get_api_context()
        k = '_AUTH_' + str(self.id) + '_' + code
        await api.redis.data.exec('SET', k, 1, ex = 300)


    ################################################################
    async def check_validation_code(self, code):
        api = get_api_context()
        k = '_AUTH_' + str(self.id) + '_' + code
        check = await api.redis.data.exec('GET', k)
        print('CHECK', check)
        if check:
            await api.redis.data.exec('DELETE', k)
            return True
        return False


    ################################################################
    async def get_unread_messages_amount(self):
        api = get_api_context()
        amount = await api.pg.club.fetchval(
            """SELECT
                    count(t1.id)
                FROM
                    messages t1
                LEFT JOIN
                    items_views t2 ON t2.item_id = t1.id
                WHERE
                    (
                        t1.target_id = $1 OR
                        t1.target_id IN (
                            SELECT group_id FROM groups_users WHERE user_id = $1
                        )
                    ) AND t2.item_id IS NULL""",
            self.id
        )
        return amount


    ################################################################
    async def get_summary(self):
        api = get_api_context()
        data = await api.pg.club.fetchrow(
            """SELECT
                    count(DISTINCT t2.contact_id) AS amount_contacts,
                    count(DISTINCT t3.group_id) AS amount_groups,
                    count(DISTINCT t4.event_id) AS amount_events
                FROM
                    users t1
                LEFT JOIN
                    users_contacts t2 ON t2.user_id = t1.id
                LEFT JOIN
                    groups_users t3 ON t3.user_id = t1.id
                LEFT JOIN
                    events_users t4 ON t4.user_id = t1.id
                WHERE
                    t1.id = $1""",
            self.id
        )
        return dict(data)
