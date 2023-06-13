import re
from random import randint
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
        self.login = ''
        self.email = ''
        self.phone = ''
        self.active = False
        self.company = ''
        self.position = ''
        self.detail = ''
        self.status = ''
        self.tags = ''
        self.interests = ''
        self.roles = []
        self._password = ''
        self.avatar = False


    ################################################################
    @classmethod
    async def search(cls, text, active_only = True, offset = None, limit = None, count = False, applicant = None, reverse = False):
        api = get_api_context()
        result = []
        amount = None
        slice_query = ''
        conditions = [ 't1.id >= 10000' ]
        condition_query = ''
        args = []
        if active_only:
            conditions.append('t1.active IS TRUE')
        if applicant is True:
            conditions.append("""'applicant' = ANY(t4.roles)""")
        if applicant is False:
            conditions.append("""'applicant' != ANY(t4.roles)""")
        if text:
            if reverse:
                conditions.append("""to_tsvector(concat_ws(' ', t1.name, t1.email, t1.phone, t3.company, t3.position, t2.interests)) @@ to_tsquery($1)""")
            else:
                conditions.append("""to_tsvector(concat_ws(' ', t1.name, t1.email, t1.phone, t3.company, t3.position, t3.detail, t2.tags)) @@ to_tsquery($1)""")
            args.append(re.sub(r'\s+', ' | ', text))
        if offset is not None and limit is not None:
            slice_query = ' OFFSET $' + str(len(args) + 1) + ' LIMIT $' + str(len(args) + 2)
            args.extend([ offset, limit ])
        if conditions:
            conditions_query = ' WHERE ' + ' AND '.join(conditions)
        data = await api.pg.club.fetch(
            """SELECT
                    t1.id, t1.time_create, t1.time_update,
                    t1.name, t1.login, t1.email, t1.phone,
                    t1.active,
                    t3.company, t3.position, t3.detail,
                    t3.status,
                    coalesce(t2.tags, '') AS tags,
                    coalesce(t2.interests, '') AS interests,
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
                        GROUP BY
                            r3.user_id
                    ) t4 ON t4.user_id = t1.id""" + conditions_query + ' ORDER BY t1.name' + slice_query,
            *args
        )
        for row in data:
            item = User()
            item.__dict__ = dict(row)
            item.check_avatar()
            result.append(item)
        if count:
            amount = len(result)
            if offset is not None and limit is not None:
                args_count = args[:len(args) - 2]
                amount = await api.pg.club.fetchval(
                    """SELECT
                            count(t1.id)
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
                                GROUP BY
                                    r3.user_id
                            ) t4 ON t4.user_id = t1.id""" + conditions_query,
                    *args_count
                )
            return (result, amount)
        return result
    

    ################################################################
    @classmethod
    async def hash(cls):
        api = get_api_context()
        data = await api.pg.club.fetch(
            """SELECT
                    id, name
                FROM
                    users
                WHERE
                    active IS TRUE AND id >= 10000
                ORDER BY
                    name"""
        )
        return {
            str(item['id']): item['name'] for item in data
        }


    ################################################################
    def reset(self):
        self.__init__()


    ################################################################
    def show(self):
        filter = { 'time_create', 'time_update', 'login', 'email', 'phone', 'roles' }
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
                        t3.status,
                        coalesce(t2.tags, '') AS tags,
                        coalesce(t2.interests, '') AS interests,
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
                    tags = $1,
                    interests = $2
                WHERE
                    user_id = $3""",
            kwargs['tags'], kwargs['interests'], self.id
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
                            t1.active,
                            t3.company, t3.position, t3.detail,
                            t3.status,
                            coalesce(t2.tags, '') AS tags,
                            coalesce(t2.interests, '') AS interests,
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
                                GROUP BY
                                    r3.user_id
                            ) t4 ON t4.user_id = t1.id
                        WHERE t1.active IS TRUE AND """ + ' AND '.join(qr),
                    *ar
                )
                if data:
                    self.__dict__ = dict(data)
                    self.check_avatar()
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
                        t1.active,
                        t3.company, t3.position, t3.detail,
                        t3.status,
                        coalesce(t2.tags, '') AS tags,
                        coalesce(t2.interests, '') AS interests,
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
                            GROUP BY
                                r3.user_id
                        ) t4 ON t4.user_id = t1.id
                    WHERE
                        (login = $1 OR email = $1 OR phone = $2) AND
                        active IS TRUE""",
                account, phone
            )
            if data and data['_password'] == password:
                self.__dict__ = dict(data)
                self.check_avatar()
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
        # print('CHECK', check)
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
                    items_views t2 ON t2.item_id = t1.id AND t2.user_id = $1
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


    ################################################################
    async def get_contacts(self):
        api = get_api_context()
        data = await api.pg.club.fetch(
            """SELECT
                    t2.id, t2.name,
                    t4.company, t4.position, t4.status,
                    coalesce(t3.tags, '') AS tags,
                    coalesce(t3.interests, '') AS interests,
                    NULL AS description,
                    NULL AS members,
                    'person' AS type
                FROM
                    users_contacts t1
                INNER JOIN
                    users t2 ON t2.id = t1.contact_id
                INNER JOIN
                    users_tags t3 ON t3.user_id = t2.id
                INNER JOIN
                    users_info t4 ON t4.user_id = t2.id
                WHERE
                    t1.user_id = $1 AND t2.active IS TRUE
                UNION ALL
                SELECT
                    t6.id, t6.name,
                    NULL AS company, NULL AS position, NULL AS status,
                    NULL AS tags,
                    NULL AS interests,
                    t6.description,
                    t7.members,
                    'group' AS type
                FROM
                    groups_users t5
                INNER JOIN
                    groups t6 ON t6.id = t5.group_id
                INNER JOIN
                    (SELECT group_id, count(user_id) AS members FROM groups_users GROUP BY group_id) t7 ON t7.group_id = t6.id
                WHERE
                    t5.user_id = $1""",
            self.id
        )
        return [ dict(item) | { 'avatar': check_avatar_by_id(item['id']) } for item in data ]


    ################################################################
    async def get_recommendations(self, amount = 2):
        api = get_api_context()
        query1 = ' | '.join([ re.sub(r'\s+', ' & ', t.strip()) for t in self.interests.split(',') ])
        query2 = ' | '.join([ re.sub(r'\s+', ' & ', t.strip()) for t in self.tags.split(',') ])
        data1 = await api.pg.club.fetch(
            """SELECT
                    id, name, company, position, status, tags, search, offer
                FROM
                    (
                        SELECT
                            t1.id, t1.name,
                            t3.company, t3.position, t3.status,
                            ts_headline(t2.tags, to_tsquery($1), 'HighlightAll=true, StartSel=~, StopSel=~') AS tags,
                            $1 AS search,
                            'bid' AS offer,
                            ts_rank_cd(to_tsvector(t2.tags), to_tsquery($1), 32) AS __rank
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
                                GROUP BY
                                    r3.user_id
                            ) t4 ON t4.user_id = t1.id
                        WHERE
                            t1.id >= 10000 AND
                            t1.id <> $2 AND
                            t1.active IS TRUE AND
                            t1.id NOT IN (
                                SELECT contact_id FROM users_contacts WHERE user_id = $2
                            ) AND
                            to_tsvector(t2.tags) @@ to_tsquery($1)
                        ORDER BY
                            __rank DESC
                        LIMIT 20
                    ) u
                ORDER BY random()
                LIMIT $3""",
            query1, self.id, amount
        )
        data2 = await api.pg.club.fetch(
            """SELECT
                    id, name, company, position, status, tags, search, offer
                FROM
                    (
                        SELECT
                            t1.id, t1.name,
                            t3.company, t3.position, t3.status,
                            ts_headline(t2.interests, to_tsquery($1), 'HighlightAll=true, StartSel=~, StopSel=~') AS tags,
                            $1 AS search,
                            'ask' AS offer,
                            ts_rank_cd(to_tsvector(t2.interests), to_tsquery($1), 32) AS __rank
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
                                GROUP BY
                                    r3.user_id
                            ) t4 ON t4.user_id = t1.id
                        WHERE
                            t1.id >= 10000 AND
                            t1.id <> $2 AND
                            t1.active IS TRUE AND
                            t1.id NOT IN (
                                SELECT contact_id FROM users_contacts WHERE user_id = $2
                            ) AND
                            to_tsvector(t2.interests) @@ to_tsquery($1)
                        ORDER BY
                            __rank DESC
                        LIMIT 20
                    ) u
                ORDER BY random()
                LIMIT $3""",
            query2, self.id, amount
        )
        return {
            'tags': [ dict(item) | { 'avatar': check_avatar_by_id(item['id']) } for item in data1 ],
            'interests': [ dict(item) | { 'avatar': check_avatar_by_id(item['id']) } for item in data2 ],
        }



    ################################################################
    async def get_suggestions(self, id = None, filter = None, today = False, from_id = None):
        api = get_api_context()
        query_tags = """SELECT
                                t1.id, t1.name, t1.time_create,
                                t3.company, t3.position, t3.status,
                                ts_headline(t2.tags, to_tsquery(${i}), 'HighlightAll=true, StartSel=~, StopSel=~') AS tags,
                                ${i} AS search,
                                'bid' AS offer,
                                ts_rank_cd(to_tsvector(t2.tags), to_tsquery(${i}), 32) AS __rank
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
                                    GROUP BY
                                        r3.user_id
                                ) t4 ON t4.user_id = t1.id
                            WHERE
                                t1.id >= 10000 AND
                                t1.active IS TRUE AND
                                to_tsvector(t2.tags) @@ to_tsquery(${i})"""
        query_interests = """SELECT
                                    t1.id, t1.name, t1.time_create,
                                    t3.company, t3.position, t3.status,
                                    ts_headline(t2.interests, to_tsquery(${i}), 'HighlightAll=true, StartSel=~, StopSel=~') AS tags,
                                    ${i} AS search,
                                    'ask' AS offer,
                                    ts_rank_cd(to_tsvector(t2.interests), to_tsquery(${i}), 32) AS __rank
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
                                        GROUP BY
                                            r3.user_id
                                    ) t4 ON t4.user_id = t1.id
                                WHERE
                                    t1.id >= 10000 AND
                                    t1.active IS TRUE AND
                                    to_tsvector(t2.interests) @@ to_tsquery(${i})"""
        i = 1
        query_parts = []
        query_offset = []
        args = []
        if filter:
            if filter == 'tags':
                query1 = ' | '.join([ re.sub(r'\s+', ' & ', t.strip()) for t in self.interests.split(',') ])
                query_parts.append(query_tags.format(i = 1))
                args.append(query1)
                i = 2
            elif filter == 'interests':
                query2 = ' | '.join([ re.sub(r'\s+', ' & ', t.strip()) for t in self.tags.split(',') ])
                query_parts.append(query_interests.format(i = 1))
                args.append(query2)
                i = 2
        else:
            query1 = ' | '.join([ re.sub(r'\s+', ' & ', t.strip()) for t in self.interests.split(',') ])
            query2 = ' | '.join([ re.sub(r'\s+', ' & ', t.strip()) for t in self.tags.split(',') ])
            query_parts.extend([ query_tags.format(i = 1), query_interests.format(i = 2) ])
            args.extend([ query1, query2 ])
            i = 3
        if id:
            query_offset.append('id > ${i} '.format(i = i))
            args.append(id)
            i += 1
        query_condition = """u.id <> ${i} AND
                    u.id NOT IN (
                        SELECT contact_id FROM users_contacts WHERE user_id = ${i}
                    )"""
        query_condition = query_condition.format(i = i)
        args.append(self.id)
        i += 1
        if from_id:
            query_condition += ' AND u.id > ${i}'.format(i = i)
            args.append(from_id)
            i += 1
        query_where = ''
        if today:
            query_offset.append('time_create >= (now() at time zone \'utc\')::date')
        if query_offset:
            query_where = ' WHERE '
        data = await api.pg.club.fetch(
            """SELECT
                    id, name, time_create, company, position, status, tags, search, offer, count(*) OVER() AS amount
                FROM
                    (
                        SELECT * FROM
                            (""" + ' UNION ALL '.join(query_parts) + """
                            ) d""" + query_where + ' AND '.join(query_offset) + """
                        ORDER BY
                            time_create DESC
                    ) u
                WHERE """ + query_condition + """ LIMIT 50""",
            *args
        )
        return [ dict(item) | { 'avatar': check_avatar_by_id(item['id']) } for item in data ]



    ################################################################
    async def add_contact(self, contact_id):
        api = get_api_context()
        data = await api.pg.club.execute(
            """INSERT INTO users_contacts (user_id, contact_id) VALUES ($1, $2) ON CONFLICT (user_id, contact_id) DO NOTHING""",
            self.id, contact_id
        )


    ################################################################
    async def del_contact(self, contact_id):
        api = get_api_context()
        data = await api.pg.club.execute(
            """DELETE FROM users_contacts WHERE user_id = $1 AND contact_id = $2""",
            self.id, contact_id
        )


    ################################################################
    async def add_event(self, event_id):
        api = get_api_context()
        data = await api.pg.club.execute(
            """INSERT INTO events_users (event_id, user_id) VALUES ($1, $2) ON CONFLICT (event_id, user_id) DO NOTHING""",
            event_id, self.id
        )


    ################################################################
    async def del_event(self, event_id):
        api = get_api_context()
        data = await api.pg.club.execute(
            """DELETE FROM events_users WHERE event_id = $1 AND user_id = $2""",
            event_id, self.id
        )


    ################################################################
    async def filter_selected_events(self, events_ids):
        api = get_api_context()
        data = await api.pg.club.fetch(
            """SELECT
                    event_id
                FROM
                    events_users
                WHERE
                    user_id = $1 AND event_id = ANY($2)""",
            self.id, tuple(events_ids)
        )
        return [ row['event_id'] for row in data ]


    ################################################################
    async def thumbsup(self, item_id):
        api = get_api_context()
        data = await api.pg.club.execute(
            """INSERT INTO items_thumbsup (item_id, user_id) VALUES ($1, $2) ON CONFLICT (item_id, user_id) DO NOTHING""",
            item_id, self.id
        )


    ################################################################
    async def filter_thumbsup(self, items_ids):
        api = get_api_context()
        data = await api.pg.club.fetch(
            """SELECT
                    item_id
                FROM
                    items_thumbsup
                WHERE
                    user_id = $1 AND item_id = ANY($2)""",
            self.id, tuple(items_ids)
        )
        return [ row['item_id'] for row in data ]


    ################################################################
    async def group_access(self, group_id):
        api = get_api_context()
        data = await api.pg.club.fetchval(
            """SELECT
                    group_id
                FROM
                    groups_users
                WHERE
                    user_id = $1 AND group_id = $2""",
            self.id, group_id
        )
        if data == group_id:
            return True
        return False


    ################################################################
    def check_avatar(self):
        self.avatar = os.path.isfile('/var/www/media.clubgermes.ru/html/avatars/' + str(self.id) + '.jpg')


    ################################################################
    def check_roles(self, roles):
        return set(self.roles) & roles


    ################################################################
    async def prepare(self, user_data, email_code, phone_code):
        # TODO: сделать полный prepare (все полня)
        api = get_api_context()
        k = '_REGISTER_' + user_data['email'] + '_' + email_code + '_' + phone_code
        await api.redis.data.exec('SET', k, data_pack(user_data, False), ex = 900)


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
        i = 1
        query = []
        args = [ id ]
        if 'tags' in kwargs:
            i += 1
            query,append('tags = $' + str(i))
            args.append(kwargs['tags'])
        if 'interests' in kwargs:
            i += 1
            query = [ 'interests = $' + str(i) ]
            args.append(kwargs['interests'])
        if query:
            await api.pg.club.execute(
                """UPDATE
                        users_tags
                    SET
                        """ + ', '.join(query) + """
                    WHERE
                        user_id = $1""",
                *args
            )
        await self.set(id = id)



################################################################
def check_avatar_by_id(id):
    return os.path.isfile('/var/www/media.clubgermes.ru/html/avatars/' + str(id) + '.jpg')



################################################################
async def validate_registration(email, email_code, phone_code):
    api = get_api_context()
    k = '_REGISTER_' + email + '_' + email_code + '_' + phone_code
    data = await api.redis.data.exec('GET', k)
    # print('DATA', data)
    if data:
        await api.redis.data.exec('DELETE', k)
        return data_unpack(data)
    return None
