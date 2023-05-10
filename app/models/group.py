import re
import os.path

from app.core.context import get_api_context
from app.utils.packager import pack as data_pack, unpack as data_unpack
from app.models.role import get_roles



####################################################################
class User:


    ################################################################
    def __init__(self):
        self.id = 0
        self.time_create = None
        self.time_update = None
        self.name = ''
        self.description = ''
        self.avatar = False


    ################################################################
    @classmethod
    async def search(cls, text, offset = None, limit = None, count = False):
        api = get_api_context()
        result = []
        slice_query = ''
        conditions = [ 't1.id >= 10000' ]
        condition_query = ''
        args = []
        if text:
            conditions.append("""to_tsvector(concat_ws(' ', name, description)) @@ to_tsquery($1)""")
            args.append(re.sub(r'\s+', ' | ', text))
        if offset and limit:
            slice_query = ' OFFSET $2 LIMIT $3'
            args.extend([ offset, limit ])
        if conditions:
            conditions_query = ' WHERE ' + ' AND '.join(conditions)
        data = await api.pg.club.fetch(
            """SELECT
                    id, time_create, time_update,
                    name, description
                FROM
                    groups""" + conditions_query + ' ORDER BY name' + slice_query,
            *args
        )
        for row in data:
            item = Group()
            item.__dict__ = dict(row)
            item.check_avatar()
            result.append(item)
        if count:
            amount = len(result)
            if offset and limit:
                amount = await api.pg.club.fetchval(
                    """SELECT
                            count(id)
                        FROM
                            groups""" + conditions_query,
                    *args
                )
            return (result, amount)
        return result
    
    
    ################################################################
    def reset(self):
        self.__init__()


    ################################################################
    def show(self):
        filter = { 'time_create', 'time_update' }
        return { k: v for k, v in self.__dict__.items() if not k.startswith('_') and k not in filter }


    ################################################################
    def dump(self):
        return { k: v for k, v in self.__dict__.items() }


    ################################################################
    async def set(self, id, active = True):
        api = get_api_context()
        if id:
            query = ''
            if active is True:
                query = ' AND active IS TRUE'
            if active is False:
                query = ' AND active IS FALSE'
            data = await api.pg.club.fetchrow(
                """SELECT
                        t1.id, t1.time_create, t1.time_update,
                        t1.name, t1.login, t1.email, t1.phone,
                        t1.active,
                        t3.company, t3.position, t3.detail,
                        t3.status, coalesce(t2.tags, '') AS tags,
                        coalesce(t4.roles, '{}'::text[]) AS roles,
                        t1.password AS _password
                    FROM
                        users t1
                    INNER JOIN
                        users_tags t2 ON t2.user_id = t1.id
                    INNER JOIN
                        users_info t3 ON t3.user_id = t1.id
                    LEFT JOIN
                        (
                            SELECT
                                r3.user_id, array_agg(r3.alias) AS roles
                            FROM
                                (
                                    SELECT
                                        r1.user_id, r2.alias
                                    FROM
                                        users_roles r1
                                    INNER JOIN
                                        roles r2 ON r2.id = r1.role_id
                                ) r3
                            WHERE
                                r3.user_id = $1
                            GROUP BY
                                r3.user_id
                        ) t4 ON t4.user_id = t1.id
                    WHERE
                        id = $1""" + query,
                id
            )
            self.__dict__ = dict(data)
            self.check_avatar()


    ################################################################
    async def update(self, **kwargs):
        api = get_api_context()
        cursor = 2
        query = []
        args = []
        for k in { 'active', 'name', 'email', 'phone', 'password' }:
            if k in kwargs:
                query.append(k + ' = $' + str(cursor))
                if k == 'phone':
                    args.append('+7' + ''.join(list(re.sub(r'[^\d]+', '', kwargs['phone']))[-10:]))
                else:
                    args.append(kwargs[k])
                cursor += 1
        if query:
            await api.pg.club.execute(
                """UPDATE
                        users
                    SET
                        """ + ', '.join(query) + """
                    WHERE
                        id = $1""",
                self.id, *args
            )
        await api.pg.club.execute(
            """UPDATE
                    users_info
                SET
                    company = $2,
                    position = $3,
                    detail = $4,
                    status = $5
                WHERE
                    user_id = $1""",
            self.id, kwargs['company'], kwargs['position'], kwargs['detail'], kwargs['status'] if 'status' in kwargs else self.status
        )
        await api.pg.club.execute(
            """UPDATE
                    users_tags
                SET
                    tags = $1
                WHERE
                    user_id = $2""",
            kwargs['tags'], self.id
        )
        if 'roles' in kwargs:
            roles = await get_roles()
            await api.pg.club.execute(
                """DELETE FROM users_roles WHERE user_id = $1""",
                self.id
            )
            cursor = 2
            query = []
            args = []
            for r in kwargs['roles']:
                query.append('($1, $' + str(cursor) + ')')
                args.append(roles[r])
                cursor += 1
            if query:
                await api.pg.club.execute(
                    """INSERT INTO
                            users_roles (user_id, role_id)
                        VALUES """ + ', '.join(query),
                    self.id, *args
                )


    ################################################################
    def copy(self, user):
        self.__dict__ = user.__dict__.copy()


    ################################################################
    def check_avatar(self):
        self.avatar = os.path.isfile('/var/www/media.clubgermes.ru/html/avatars/' + str(self.id) + '.jpg')


    ################################################################
    async def create(self, **kwargs):
        # TODO: сделать полный register (все поля)
        api = get_api_context()
        # только мобильники рф
        id = await api.pg.club.fetchval(
            """INSERT INTO
                    users (name, email, phone, password, active)
                VALUES
                    ($1, $2, $3, $4, $5)
                RETURNING
                    id""",
            kwargs['name'],
            kwargs['email'],
            '+7' + ''.join(list(re.sub(r'[^\d]+', '', kwargs['phone']))[-10:]),
            kwargs['password'],
            kwargs['active'] if 'active' in kwargs else True,
        )
        await api.pg.club.execute(
            """UPDATE
                    users_info
                SET
                    company = $2,
                    position = $3,
                    detail = $4,
                    status = $5
                WHERE
                    user_id = $1""",
            id,
            kwargs['company'],
            kwargs['position'], 
            kwargs['detail'] if 'detail' in kwargs else '',
            kwargs['status'] if 'status' in kwargs else 'бронзовый'
        )
        roles = await get_roles()
        if kwargs['roles']:
            await api.pg.club.execute(
                """INSERT INTO
                        users_roles (user_id, role_id)
                    VALUES
                        ($1, $2)""",
                id, kwargs['roles'][0] if type(kwargs['roles'][0]) == int else roles[kwargs['roles'][0]]
            )
        if 'tags' in kwargs:
            await api.pg.club.execute(
                """UPDATE
                        users_tags
                    SET
                        tags = $1
                    WHERE
                        user_id = $2""",
                kwargs['tags'], id
            )
        await self.set(id = id)



################################################################
def check_avatar_by_id(id):
    return os.path.isfile('/var/www/media.clubgermes.ru/html/avatars/' + str(id) + '.jpg')
