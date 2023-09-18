import re
import os.path

from app.core.context import get_api_context
from app.utils.packager import pack as data_pack, unpack as data_unpack
from app.models.role import get_roles



####################################################################
class Group:


    ################################################################
    def __init__(self):
        self.id = 0
        self.time_create = None
        self.time_update = None
        self.name = ''
        self.description = ''
        self.avatar_hash = None
        self.users = []


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
            conditions.append("""to_tsvector(concat_ws(' ', t1.name, t1.description)) @@ to_tsquery($1)""")
            args.append(re.sub(r'\s+', ' | ', text))
        if offset and limit:
            slice_query = ' OFFSET $2 LIMIT $3'
            args.extend([ offset, limit ])
        if conditions:
            conditions_query = ' WHERE ' + ' AND '.join(conditions)
        data = await api.pg.club.fetch(
            """SELECT
                    t1.id, t1.time_create, t1.time_update,
                    t1.name, t1.description, coalesce(t2.users, '{}'::bigint[]) AS users,
                    t8.hash AS avatar_hash
                FROM
                    groups t1
                LEFT JOIN
                    avatars t8 ON t8.owner_id = t1.id AND t8.active IS TRUE
                LEFT JOIN
                    (
                        SELECT
                            group_id, array_agg(user_id) AS users
                        FROM
                            groups_users
                        GROUP BY
                            group_id
                    ) t2 ON t2.group_id = t1.id""" + conditions_query + ' ORDER BY t1.name' + slice_query,
            *args
        )
        for row in data:
            item = Group()
            item.__dict__ = dict(row)
            result.append(item)
        if count:
            amount = len(result)
            if offset and limit:
                amount = await api.pg.club.fetchval(
                    """SELECT
                            count(t1.id)
                        FROM
                            groups t1""" + conditions_query,
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
    async def set(self, id):
        api = get_api_context()
        if id:
            data = await api.pg.club.fetchrow(
                """SELECT
                        t1.id, t1.time_create, t1.time_update,
                        t1.name, t1.description, coalesce(t2.users, '{}'::bigint[]) AS users,
                        t8.hash AS avatar_hash
                    FROM
                        groups t1
                    LEFT JOIN
                        avatars t8 ON t8.owner_id = t1.id AND t8.active IS TRUE
                    LEFT JOIN
                        (
                            SELECT
                                group_id, array_agg(user_id) AS users
                            FROM
                                groups_users
                            GROUP BY
                                group_id
                        ) t2 ON t2.group_id = t1.id
                    WHERE
                        t1.id = $1""",
                id
            )
            self.__dict__ = dict(data)


    ################################################################
    async def update(self, **kwargs):
        api = get_api_context()
        cursor = 2
        query = []
        args = []
        for k in { 'name', 'description' }:
            if k in kwargs:
                query.append(k + ' = $' + str(cursor))
                args.append(kwargs[k])
                cursor += 1
        if query:
            await api.pg.club.execute(
                """UPDATE
                        groups
                    SET
                        """ + ', '.join(query) + """
                    WHERE
                        id = $1""",
                self.id, *args
            )
        if 'users' in kwargs:
            ids_to_delete = set(self.users) - set(kwargs['users'])
            if ids_to_delete:
                await api.pg.club.execute(
                    """DELETE FROM groups_users WHERE group_id = $1 AND user_id = ANY($2)""",
                    self.id, list(ids_to_delete)
                )
            ids_to_add = set(kwargs['users']) - set(self.users)
            if ids_to_add:
                cursor = 2
                query = []
                args = []
                for u in ids_to_add:
                    query.append('($1, $' + str(cursor) + ')')
                    args.append(u)
                    cursor += 1
                await api.pg.club.execute(
                    """INSERT INTO
                            groups_users (group_id, user_id)
                        VALUES """ + ', '.join(query),
                    self.id, *args
                )


    ################################################################
    def copy(self, user):
        self.__dict__ = user.__dict__.copy()



    ################################################################
    async def create(self, **kwargs):
        api = get_api_context()
        id = await api.pg.club.fetchval(
            """INSERT INTO
                    groups (name, description)
                VALUES
                    ($1, $2)
                RETURNING
                    id""",
            kwargs['name'],
            kwargs['description']
        )
        await self.set(id = id)
