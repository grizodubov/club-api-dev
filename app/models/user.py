import re
from random import randint, choice
import string
import os.path

from app.core.context import get_api_context
from app.utils.packager import pack as data_pack, unpack as data_unpack
from app.models.role import get_roles
from app.models.message import check_recepient, check_recepients



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
        self.annual = ''
        self.annual_privacy = ''
        self.employees = ''
        self.employees_privacy = ''
        self.catalog = ''
        self.city = ''
        self.hobby = ''
        self.birthdate = None
        self.birthdate_privacy = ''
        self.experience = None
        self.tags = ''
        self.interests = ''
        self.rating = 0
        self.roles = []
        self._password = ''
        self.avatar_hash = None
        self.online = False
        self.community_manager_id = 0
        self.link_telegram = ''
        self.id_telegram = None


    ################################################################
    @classmethod
    async def search(cls, text, active_only = True, offset = None, limit = None, count = False, applicant = False, reverse = False, target = None):
        api = get_api_context()
        result = []
        amount = None
        slice_query = ''
        conditions = [ 't1.id >= 10000' ]
        condition_query = ''
        args = []
        if active_only:
            conditions.append('t1.active IS TRUE')
        if applicant is None:
            applicant = False
        if applicant is True:
            conditions.append("""'applicant' = ANY(t4.roles)""")
        if applicant is False:
            conditions.append("""'applicant' <> ANY(t4.roles)""")
        if text:
            if target:
                if target == 'tags':
                    conditions.append("""regexp_split_to_array($1, '\s*,\s*') && regexp_split_to_array(t2.tags, '\s*,\s*')""")
                else:
                    conditions.append("""regexp_split_to_array($1, '\s*,\s*') && regexp_split_to_array(t2.interests, '\s*,\s*')""")
                args.append(text)
            else:
                if reverse:
                    conditions.append("""(to_tsvector(concat_ws(' ', t1.name, t1.email, t1.phone, t3.company, t3.position, t2.interests)) @@ to_tsquery($1) OR t1.name ILIKE concat_ws('%', $1, '%'))""")
                else:
                    conditions.append("""(to_tsvector(concat_ws(' ', t1.name, t1.email, t1.phone, t3.company, t3.position, t3.detail, t2.tags, t2.interests)) @@ to_tsquery($1) OR t1.name ILIKE concat_ws('%', $1, '%'))""")
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
                    t1.community_manager_id,
                    t3.company, t3.position, t3.detail,
                    t3.status,
                    t3.annual, t3.annual_privacy,
                    t3.employees, t3.employees_privacy,
                    t3.catalog, t3.city, t3.hobby,
                    t3.link_telegram, t3.id_telegram,
                    to_char(t3.birthdate, 'DD/MM/YYYY') AS birthdate, t3.birthdate_privacy,
                    t3.experience,
                    coalesce(t2.tags, '') AS tags,
                    coalesce(t2.interests, '') AS interests,
                    t5.hash AS avatar_hash,
                    coalesce(t4.roles, '{}'::text[]) AS roles,
                    t1.password AS _password
                FROM
                    users t1
                INNER JOIN
                    users_tags t2 ON t2.user_id = t1.id
                INNER JOIN
                    users_info t3 ON t3.user_id = t1.id
                LEFT JOIN
                    avatars t5 ON t5.owner_id = t1.id AND t5.active IS TRUE
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
            item.check_online()
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
    async def client_search(cls, text, ids = [], active_only = True, offset = None, limit = None, count = False, applicant = False, reverse = False, target = None):
        api = get_api_context()
        result = []
        amount = None
        slice_query = ''
        conditions = [ 't1.id >= 10000' ]
        condition_query = ''
        args = []
        if active_only:
            conditions.append('t1.active IS TRUE')
        if applicant is None:
            applicant = False
        if applicant is True:
            conditions.append("""'applicant' = ANY(t4.roles)""")
        if applicant is False:
            conditions.append("""'applicant' <> ANY(t4.roles)""")
        if text:
            if target:
                if target == 'tags':
                    conditions.append("""regexp_split_to_array($1, '\s*,\s*') && regexp_split_to_array(t2.tags, '\s*,\s*')""")
                else:
                    conditions.append("""regexp_split_to_array($1, '\s*,\s*') && regexp_split_to_array(t2.interests, '\s*,\s*')""")
                args.append(text)
            else:
                if reverse:
                    conditions.append("""(to_tsvector(concat_ws(' ', t1.name, t1.email, t1.phone, t3.company, t3.position, t2.interests)) @@ to_tsquery($1) OR t1.name ILIKE concat_ws('%', $1, '%'))""")
                else:
                    conditions.append("""(to_tsvector(concat_ws(' ', t1.name, t1.email, t1.phone, t3.company, t3.position, t3.detail, t2.tags, t2.interests)) @@ to_tsquery($1) OR t1.name ILIKE concat_ws('%', $1, '%'))""")
                args.append(re.sub(r'\s+', ' | ', text))
        if ids:
            conditions.append("""t1.id = ANY($""" + str(len(args) + 1) + """)""")
            args.append(ids)
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
                    t1.community_manager_id,
                    t3.company, t3.position, t3.detail,
                    t3.status,
                    t3.annual, t3.annual_privacy,
                    t3.employees, t3.employees_privacy,
                    t3.catalog, t3.city, t3.hobby,
                    t3.link_telegram, t3.id_telegram,
                    to_char(t3.birthdate, 'DD/MM/YYYY') AS birthdate, t3.birthdate_privacy,
                    t3.experience,
                    coalesce(t2.tags, '') AS tags,
                    coalesce(t2.interests, '') AS interests,
                    t5.hash AS avatar_hash,
                    coalesce(t4.roles, '{}'::text[]) AS roles,
                    t1.password AS _password
                FROM
                    users t1
                INNER JOIN
                    users_tags t2 ON t2.user_id = t1.id
                INNER JOIN
                    users_info t3 ON t3.user_id = t1.id
                INNER JOIN
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
                    ) t4 ON t4.user_id = t1.id AND 'client'::text = ANY(t4.roles)
                LEFT JOIN
                    avatars t5 ON t5.owner_id = t1.id AND t5.active IS TRUE""" + conditions_query + ' ORDER BY t1.name' + slice_query,
            *args
        )
        for row in data:
            item = User()
            item.__dict__ = dict(row)
            item.check_online()
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
    async def for_select(cls):
        api = get_api_context()
        data = await api.pg.club.fetch(
            """SELECT
                    id, name
                FROM
                    users
                WHERE
                    id >= 10000
                ORDER BY
                    name"""

        )
        return ( [ dict(item) for item in data ], len(data) )


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
        filter = { 'time_create', 'time_update', 'community_manager_id', 'login', 'email', 'phone', 'roles', 'annual', 'annual_privacy', 'employees', 'employees_privacy', 'birthdate', 'birthdate_privacy' }
        data = { k: v for k, v in self.__dict__.items() if not k.startswith('_') and k not in filter }
        # annual
        if self.annual_privacy == 'показывать':
            data['annual'] = self.annual if self.annual else 'не указано'
        elif self.annual_privacy == 'показывать диапазон':
            temp = re.sub(r'[^0-9]+', '', self.annual)
            if temp:
                val = int(temp)
                if val <= 1000000:
                    data['annual'] = 'до 1 млн.'
                elif val <= 10000000:
                    data['annual'] = 'до 10 млн.'
                elif val <= 100000000:
                    data['annual'] = 'до 100 млн.'
                elif val <= 1000000000:
                    data['annual'] = 'до 1 млрд.'
                elif val <= 10000000000:
                    data['annual'] = 'до 10 млрд.'
                elif val <= 100000000000:
                    data['annual'] = 'до 100 млрд.'
                elif val <= 1000000000000:
                    data['annual'] = 'до 1 трлн.'
                elif val > 1000000000000:
                    data['annual'] = 'больше 1 трлн.'
            else:
                data['annual'] = 'не указано'
        else:
            data['annual'] = 'скрыто'
        # employees
        if self.employees_privacy == 'показывать':
            data['employees'] = self.employees if self.employees else 'не указано'
        elif self.employees_privacy == 'показывать диапазон':
            temp = re.sub(r'[^0-9]+', '', self.employees)
            if temp:
                val = int(temp)
                if val <= 10:
                    data['employees'] = '1 - 10'
                elif val > 10 and val <= 100:
                    data['employees'] = '11 - 100'
                elif val > 100 and val <= 200:
                    data['employees'] = '101 - 200'
                elif val > 200 and val <= 500:
                    data['employees'] = '201 - 500'
                elif val > 500 and val <= 1000:
                    data['employees'] = '501 - 1000'
                elif val > 1000:
                    data['employees'] = '1000+'
            else:
                data['employees'] = 'не указано'
        else:
            data['employees'] = 'скрыто'
        # birthdate
        if self.birthdate_privacy == 'показывать':
            data['birthdate'] = self.birthdate if self.birthdate else 'не указано'
        elif self.birthdate_privacy == 'показывать год':
            data['birthdate'] = self.birthdate[-4:] if self.birthdate else 'не указано'
        else:
            data['birthdate'] = 'скрыто'
        return data


    ################################################################
    def dshow(self):
        filter = { 'time_create', 'time_update', 'login', 'email', 'phone', 'roles', 'id_telegram' }
        data = { k: v for k, v in self.__dict__.items() if not k.startswith('_') and k not in filter }
        return data


    ################################################################
    def dump(self):
        return { k: v for k, v in self.__dict__.items() }


    ################################################################
    async def set(self, id, active = True):
        api = get_api_context()
        if id:
            query = ''
            if active is True:
                query = ' AND t1.active IS TRUE'
            if active is False:
                query = ' AND t1.active IS FALSE'
            data = await api.pg.club.fetchrow(
                """SELECT
                        t1.id, t1.time_create, t1.time_update,
                        t1.name, t1.login, t1.email, t1.phone,
                        t1.active,
                        t1.community_manager_id,
                        t3.company, t3.position, t3.detail,
                        t3.status,
                        t3.annual, t3.annual_privacy,
                        t3.employees, t3.employees_privacy,
                        t3.catalog, t3.city, t3.hobby,
                        t3.link_telegram, t3.id_telegram,
                        to_char(t3.birthdate, 'DD/MM/YYYY') AS birthdate, t3.birthdate_privacy,
                        t3.experience,
                        coalesce(t2.tags, '') AS tags,
                        coalesce(t2.interests, '') AS interests,
                        t5.hash AS avatar_hash,
                        coalesce(t4.roles, '{}'::text[]) AS roles,
                        t1.password AS _password
                    FROM
                        users t1
                    INNER JOIN
                        users_tags t2 ON t2.user_id = t1.id
                    INNER JOIN
                        users_info t3 ON t3.user_id = t1.id
                    LEFT JOIN
                        avatars t5 ON t5.owner_id = t1.id AND t5.active IS TRUE
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
                        t1.id = $1""" + query,
                id
            )
            if data:
                self.__dict__ = dict(data)
                self.check_online()


    ################################################################
    async def update(self, **kwargs):
        api = get_api_context()
        cursor = 2
        query = []
        args = []
        for k in { 'active', 'name', 'email', 'phone', 'password', 'community_manager_id' }:
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
                    status = $5,

                    annual = $6,
                    annual_privacy = $7,
                    employees = $8,
                    employees_privacy = $9,
                    catalog = $10,
                    city = $11,
                    hobby = $12,
                    birthdate = $13,
                    birthdate_privacy = $14,
                    experience = $15,
                    link_telegram = $16
                WHERE
                    user_id = $1""",
            self.id, kwargs['company'], kwargs['position'], kwargs['detail'], kwargs['status'] if 'status' in kwargs else self.status,
            kwargs['annual'] if 'annual' in kwargs else '',
            kwargs['annual_privacy'] if 'annual_privacy' in kwargs else '',
            kwargs['employees'] if 'employees' in kwargs else '',
            kwargs['employees_privacy'] if 'employees_privacy' in kwargs else '',
            kwargs['catalog'] if 'catalog' in kwargs else '',
            kwargs['city'] if 'city' in kwargs else '',
            kwargs['hobby'] if 'hobby' in kwargs else '',
            kwargs['birthdate'] if 'birthdate' in kwargs else None,
            kwargs['birthdate_privacy'] if 'birthdate_privacy' in kwargs else '',
            kwargs['experience'] if 'experience' in kwargs else None,
            kwargs['link_telegram'] if 'link_telegram' in kwargs else ''
        )
                
        tags_old = set(sorted(re.split(r'\s*,\s*', self.tags)))
        interests_old = set(sorted(re.split(r'\s*,\s*', self.interests)))
        # print('OLD', tags_old, interests_old)
        tags_new = set(sorted(re.split(r'\s*,\s*', kwargs['tags'] if 'tags' in kwargs and kwargs['tags'] else '')))
        interests_new = set(sorted(re.split(r'\s*,\s*', kwargs['interests'] if 'interests' in kwargs and kwargs['interests'] else '')))
        # print('NEW', tags_new, interests_new)
        update_i = 1
        update_pams = []
        update_args = []
        if tags_old != tags_new:
            update_pams.extend([
                'tags = $' + str(update_i),
                'time_update_tags = now() at time zone \'utc\'',
            ])
            temp = None
            if kwargs['tags'].strip():
                temp = ','.join([ t for t in re.split(r'\s*,\s*', kwargs['tags'].strip()) if t ])
            update_args.append(temp)
            update_i += 1
        if interests_old != interests_new:
            update_pams.extend([
                'interests = $' + str(update_i),
                'time_update_interests = now() at time zone \'utc\'',
            ])
            temp = None
            if kwargs['interests'].strip():
                temp = ','.join([ t for t in re.split(r'\s*,\s*', kwargs['interests'].strip()) if t ])
            update_args.append(temp)
            update_i += 1
        if update_pams:
            update_args.append(self.id)
            await api.pg.club.execute(
                """UPDATE
                        users_tags
                    SET
                    """ + ', '.join(update_pams) + """
                    WHERE
                        user_id = $""" + str(update_i),
                *update_args
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
                            t1.community_manager_id,
                            t3.company, t3.position, t3.detail,
                            t3.status,
                            t3.annual, t3.annual_privacy,
                            t3.employees, t3.employees_privacy,
                            t3.catalog, t3.city, t3.hobby,
                            t3.link_telegram, t3.id_telegram,
                            to_char(t3.birthdate, 'DD/MM/YYYY') AS birthdate, t3.birthdate_privacy,
                            t3.experience,
                            coalesce(t2.tags, '') AS tags,
                            coalesce(t2.interests, '') AS interests,
                            t5.hash AS avatar_hash,
                            coalesce(t4.roles, '{}'::text[]) AS roles,
                            t1.password AS _password
                        FROM
                            users t1
                        INNER JOIN
                            users_tags t2 ON t2.user_id = t1.id
                        INNER JOIN
                            users_info t3 ON t3.user_id = t1.id
                        LEFT JOIN
                            avatars t5 ON t5.owner_id = t1.id AND t5.active IS TRUE
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
                    self.check_online()
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
                        t1.community_manager_id,
                        t3.company, t3.position, t3.detail,
                        t3.status,
                        t3.annual, t3.annual_privacy,
                        t3.employees, t3.employees_privacy,
                        t3.catalog, t3.city, t3.hobby,
                        t3.link_telegram, t3.id_telegram,
                        to_char(t3.birthdate, 'DD/MM/YYYY') AS birthdate, t3.birthdate_privacy,
                        t3.experience,
                        coalesce(t2.tags, '') AS tags,
                        coalesce(t2.interests, '') AS interests,
                        t5.hash AS avatar_hash,
                        coalesce(t4.roles, '{}'::text[]) AS roles,
                        t1.password AS _password
                    FROM
                        users t1
                    INNER JOIN
                        users_tags t2 ON t2.user_id = t1.id
                    INNER JOIN
                        users_info t3 ON t3.user_id = t1.id
                    LEFT JOIN
                        avatars t5 ON t5.owner_id = t1.id AND t5.active IS TRUE
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
                        t1.active IS TRUE""",
                account, phone
            )
            if data and data['_password'] == password:
                self.__dict__ = dict(data)
                self.check_online()
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
                    events_users t4 ON t4.user_id = t1.id AND t4.event_id IN
                        (
                            SELECT id FROM events WHERE time_event >= (now() at time zone 'utc')::date
                        )
                WHERE
                    t1.id = $1""",
            self.id
        )
        return dict(data)



    ################################################################
    async def get_helpful_answers(self):
        api = get_api_context()
        data = await api.pg.club.fetch(
            """SELECT
                    t1.id AS answer_id, t1.text AS answer_text,
                    t2.id AS question_id,
                    t2.text AS question_text,
                    t3.name AS question_author_name,
                    t1.community_id AS community_id,
                    t4.name AS community_name
                FROM
                    posts t1
                INNER JOIN
                    posts t2 ON t2.id = t1.reply_to_post_id
                INNER JOIN
                    users t3 ON t3.id = t2.author_id
                INNER JOIN
                    communities t4 ON t4.id = t1.community_id
                WHERE
                    t1.author_id = $1 AND t1.helpful IS TRUE
                ORDER BY
                    t1.id""",
            self.id
        )
        result = []
        questions = {}
        for item in data:
            k = str(item['question_id'])
            if k not in questions:
                questions[k] = {
                    'question_id': item['question_id'],
                    'question_text': item['question_text'],
                    'question_author_name': item['question_author_name'],
                    'community_name': item['community_name'],
                    'community_id': item['community_id'],
                    'answers': [
                        {
                            'answer_id': item['answer_id'],
                            'answer_text': item['answer_text'],
                        },
                    ],
                }
                result.append(questions[k])
            else:
                questions[k]['answers'].append({
                        'answer_id': item['answer_id'],
                        'answer_text': item['answer_text'],
                })
        return result



    ################################################################
    async def get_contacts(self):
        api = get_api_context()
        data = await api.pg.club.fetch(
            """SELECT
                    t2.id, t2.name,
                    t4.company, t4.position, t4.status,
                    t4.link_telegram,
                    coalesce(t3.tags, '') AS tags,
                    coalesce(t3.interests, '') AS interests,
                    t8.hash AS avatar_hash,
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
                LEFT JOIN
                    avatars t8 ON t8.owner_id = t2.id AND t8.active IS TRUE
                WHERE
                    t1.user_id = $1 AND t2.active IS TRUE
                UNION ALL
                SELECT
                    t6.id, t6.name,
                    NULL AS company, NULL AS position, NULL AS status,
                    NULL AS link_telegram,
                    NULL AS tags,
                    NULL AS interests,
                    t9.hash AS avatar_hash,
                    t6.description,
                    t7.members,
                    'group' AS type
                FROM
                    groups_users t5
                INNER JOIN
                    groups t6 ON t6.id = t5.group_id
                INNER JOIN
                    (SELECT group_id, count(user_id) AS members FROM groups_users GROUP BY group_id) t7 ON t7.group_id = t6.id
                LEFT JOIN
                    avatars t9 ON t9.owner_id = t6.id AND t9.active IS TRUE
                WHERE
                    t5.user_id = $1""",
            self.id
        )
        return [ dict(item) | { 'online': check_online_by_id(item['id']) } for item in data ]


    ################################################################
    async def get_recommendations(self, amount = 2):
        api = get_api_context()
        query1 = ' | '.join([ re.sub(r'\s+', ' & ', t.strip()) for t in self.interests.split(',') ])
        query2 = ' | '.join([ re.sub(r'\s+', ' & ', t.strip()) for t in self.tags.split(',') ])
        data1 = await api.pg.club.fetch(
            """SELECT
                    id, name, company, position, status, link_telegram, tags, search, offer, avatar_hash
                FROM
                    (
                        SELECT
                            t1.id, t1.name,
                            t3.company, t3.position, t3.status,
                            t3.link_telegram,
                            ts_headline(t2.tags, to_tsquery($1), 'HighlightAll=true, StartSel=~, StopSel=~') AS tags,
                            $1 AS search,
                            'bid' AS offer,
                            t8.hash AS avatar_hash,
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
                        LEFT JOIN
                            avatars t8 ON t8.owner_id = t1.id AND t8.active IS TRUE
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
                    id, name, company, position, status, link_telegram, tags, search, offer, avatar_hash
                FROM
                    (
                        SELECT
                            t1.id, t1.name,
                            t3.company, t3.position, t3.status,
                            t3.link_telegram,
                            ts_headline(t2.interests, to_tsquery($1), 'HighlightAll=true, StartSel=~, StopSel=~') AS tags,
                            $1 AS search,
                            'ask' AS offer,
                            t8.hash AS avatar_hash,
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
                        LEFT JOIN
                            avatars t8 ON t8.owner_id = t1.id AND t8.active IS TRUE
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
            'tags': [ dict(item) | { 'online': check_online_by_id(item['id']) } for item in data1 ],
            'interests': [ dict(item) | { 'online': check_online_by_id(item['id']) } for item in data2 ],
        }



    ################################################################
    async def get_suggestions(self, id = None, filter = None, today_offset = None, from_id = None):
        api = get_api_context()
        query_tags = """SELECT
                                t1.id, t1.name, t2.time_update_tags AS time_create,
                                t3.company, t3.position, t3.status,
                                t3.link_telegram,
                                ts_headline(t2.tags, to_tsquery(${i}), 'HighlightAll=true, StartSel=~, StopSel=~') AS tags,
                                ${i} AS search,
                                'bid' AS offer,
                                t8.hash AS avatar_hash,
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
                            LEFT JOIN
                                avatars t8 ON t8.owner_id = t1.id AND t8.active IS TRUE
                            WHERE
                                t1.id >= 10000 AND
                                t1.active IS TRUE AND
                                to_tsvector(t2.tags) @@ to_tsquery(${i})"""
        query_interests = """SELECT
                                    t1.id, t1.name, t2.time_update_interests AS time_create,
                                    t3.company, t3.position, t3.status,
                                    t3.link_telegram,
                                    ts_headline(t2.interests, to_tsquery(${i}), 'HighlightAll=true, StartSel=~, StopSel=~') AS tags,
                                    ${i} AS search,
                                    'ask' AS offer,
                                    t8.hash AS avatar_hash,
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
                                LEFT JOIN
                                    avatars t8 ON t8.owner_id = t1.id AND t8.active IS TRUE
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
        if today_offset:
            #query_offset.append('time_create >= (now() at time zone \'utc\')::date')
            query_condition += ' AND u.time_create >= (now() at time zone \'utc\')::date + (${i} * interval \'1 minute\')'.format(i = i)
            args.append(today_offset)
            i += 1
        if query_offset:
            query_where = ' WHERE '
        data = await api.pg.club.fetch(
            """SELECT
                    id, name, time_create, company, position, status, link_telegram, tags, search, offer, avatar_hash, t5.amount AS helpful, count(*) OVER() AS amount
                FROM
                    (
                        SELECT * FROM
                            (""" + ' UNION ALL '.join(query_parts) + """
                            ) d""" + query_where + ' AND '.join(query_offset) + """
                        ORDER BY
                            time_create DESC
                    ) u
                LEFT JOIN
                (
                    SELECT author_id, count(id) AS amount FROM posts WHERE helpful IS TRUE GROUP BY author_id
                ) t5 ON t5.author_id = u.id
                WHERE """ + query_condition + """ LIMIT 100""",
            *args
        )
        return [ dict(item) | { 'online': check_online_by_id(item['id']) } for item in data ]



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
    async def confirm_event(self, event_id):
        api = get_api_context()
        await api.pg.club.execute(
            """UPDATE events_users SET confirmation = TRUE WHERE event_id = $1 AND user_id = $2""",
            event_id, self.id
        )


    ################################################################
    async def filter_selected_events(self, events_ids):
        api = get_api_context()
        data = await api.pg.club.fetch(
            """SELECT
                    event_id, confirmation
                FROM
                    events_users
                WHERE
                    user_id = $1 AND event_id = ANY($2)""",
            self.id, tuple(events_ids)
        )
        return [ { 'event_id': str(row['event_id']), 'confirmation': row['confirmation'] } for row in data ]


    ################################################################
    async def thumbsup(self, item_id):
        api = get_api_context()
        data = await api.pg.club.execute(
            """INSERT INTO items_thumbsup (item_id, user_id) VALUES ($1, $2) ON CONFLICT (item_id, user_id) DO NOTHING""",
            item_id, self.id
        )



    ################################################################
    async def thumbsoff(self, item_id):
        api = get_api_context()
        data = await api.pg.club.execute(
            """DELETE FROM items_thumbsup WHERE item_id = $1 AND user_id = $2""",
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
    def check_online(self):
        api = get_api_context()
        self.online = True if self.id in api.users_online() else False


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
    async def prepare_new(self, user_data, email_code, phone_code):
        # TODO: сделать полный prepare (все полня)
        api = get_api_context()
        k = '_REGISTER_NEW_' + user_data['email'] + '_' + email_code + '_' + phone_code
        await api.redis.data.exec('SET', k, data_pack(user_data, False), ex = 1800)


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
        #print(kwargs['birthdate'])
        await api.pg.club.execute(
            """UPDATE
                    users_info
                SET
                    company = $2,
                    position = $3,
                    detail = $4,
                    status = $5,

                    annual = $6,
                    annual_privacy = $7,
                    employees = $8,
                    employees_privacy = $9,
                    catalog = $10,
                    city = $11,
                    hobby = $12,
                    birthdate = $13,
                    birthdate_privacy = $14,
                    experience = $15,
                    link_telegram = $16
                WHERE
                    user_id = $1""",
            id,
            kwargs['company'],
            kwargs['position'], 
            kwargs['detail'] if 'detail' in kwargs else '',
            kwargs['status'] if 'status' in kwargs else 'бронзовый',
            kwargs['annual'] if 'annual' in kwargs else '',
            kwargs['annual_privacy'] if 'annual_privacy' in kwargs else '',
            kwargs['employees'] if 'employees' in kwargs else '',
            kwargs['employees_privacy'] if 'employees_privacy' in kwargs else '',
            kwargs['catalog'] if 'catalog' in kwargs else '',
            kwargs['city'] if 'city' in kwargs else '',
            kwargs['hobby'] if 'hobby' in kwargs else '',
            kwargs['birthdate'] if 'birthdate' in kwargs else None,
            kwargs['birthdate_privacy'] if 'birthdate_privacy' in kwargs else '',
            kwargs['experience'] if 'experience' in kwargs else None,
            kwargs['link_telegram'] if 'link_telegram' in kwargs else ''
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
        temp_tags = None
        if 'tags' in kwargs and kwargs['tags'] and kwargs['tags'].strip():
            temp_tags = ','.join([ t for t in re.split(r'\s*,\s*', kwargs['tags'].strip()) if t ])
        temp_interests = None
        if 'interests' in kwargs and kwargs['interests'] and kwargs['interests'].strip():
            temp_interests = ','.join([ t for t in re.split(r'\s*,\s*', kwargs['interests'].strip()) if t ])
        await api.pg.club.execute(
            """UPDATE
                    users_tags
                SET
                    tags = $1,
                    interests = $2
                WHERE
                    user_id = $3""",
            temp_tags,
            temp_interests,
            id
        )
        await self.set(id = id)


    ################################################################
    async def check_access(self, user):
        if self.status == 'золотой':
            return True
        if self.status == 'серебряный' and (user.status == 'серебряный' or user.status == 'бронзовый'):
            return True
        if self.status == 'бронзовый' and user.status == 'бронзовый':
            return True
        return await check_recepient(self.id, user.id)


    ################################################################
    async def check_multiple_access(self, users):
        result = {}
        recepients_ids = []
        for user in users:
            if self.status == 'золотой':
                result[str(user.id)] = True
                continue
            if self.status == 'серебряный' and (user.status == 'серебряный' or user.status == 'бронзовый'):
                result[str(user.id)] = True
                continue
            if self.status == 'бронзовый' and user.status == 'бронзовый':
                result[str(user.id)] = True
                continue
            recepients_ids.append(user.id)
        if recepients_ids:
            data = await check_recepients(self.id, recepients_ids)
            for id in recepients_ids:
                if str(id) in data and data[str(id)] is True:
                    result[str(id)] = True
                else:
                    result[str(id)] = False
        return result


    ################################################################
    async def terminate(self):
        api = get_api_context()
        data = await api.pg.club.execute(
            """UPDATE users SET active = FALSE WHERE id = $1""",
            self.id
        )


    ################################################################
    async def update_telegram(self, telegram_id):
        api = get_api_context()
        data = await api.pg.club.execute(
            """UPDATE users_info SET id_telegram = $2 WHERE user_id = $1""",
            self.id, telegram_id
        )



################################################################
def check_online_by_id(id):
    api = get_api_context()
    if id in api.users_online():
        return True
    return False



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



################################################################
async def validate_registration_new(email, email_code, phone_code):
    api = get_api_context()
    k = '_REGISTER_NEW_' + email + '_' + email_code + '_' + phone_code
    data = await api.redis.data.exec('GET', k)
    # print('DATA', data)
    if data:
        await api.redis.data.exec('DELETE', k)
        return data_unpack(data)
    return None



################################################################
async def get_residents(users_ids = None):
    api = get_api_context()
    result = []
    query = ''
    args = []
    if users_ids:
        query = ' AND t1.id = ANY($1)'
        args.append(users_ids)
    data = await api.pg.club.fetch(
        """SELECT
                t1.id, t1.time_create, t1.time_update,
                t1.name, t1.login, t1.email, t1.phone,
                t1.active,
                t3.company, t3.position, t3.detail,
                t3.status,
                t3.link_telegram,
                t8.hash AS avatar_hash,
                t3.annual, t3.annual_privacy,
                t3.employees, t3.employees_privacy,
                t3.catalog, t3.city, t3.hobby,
                to_char(t3.birthdate, 'DD/MM/YYYY') AS birthdate, t3.birthdate_privacy,
                t3.experience,
                coalesce(t2.tags, '') AS tags,
                coalesce(t2.interests, '') AS interests,
                coalesce(t4.roles, '{}'::text[]) AS roles,
                coalesce(t5.amount, 0) AS rating,
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
            LEFT JOIN
                (
                    SELECT author_id, count(id) AS amount FROM posts WHERE helpful IS TRUE GROUP BY author_id
                ) t5 ON t5.author_id = t1.id
            LEFT JOIN
                avatars t8 ON t8.owner_id = t1.id AND t8.active IS TRUE
            WHERE
                ('client' = ANY(t4.roles) OR t1.id = 10004)""" + query + """
            ORDER BY t1.name""",
        *args
    )
    for row in data:
        item = User()
        item.__dict__ = dict(row)
        item.check_online()
        result.append(item)
    return result



################################################################
async def get_residents_contacts(user_id, user_status, contacts_ids):
    api = get_api_context()
    data = await api.pg.club.fetch(
        """SELECT
                t1.id AS user_id,
                CASE WHEN t3.user_id IS NULL THEN FALSE ELSE TRUE END AS contact,
                t2.status
            FROM
                users t1
            INNER JOIN
                users_info t2 ON t2.user_id = t1.id
            LEFT JOIN
                users_contacts t3 ON t3.contact_id = t1.id AND t3.user_id = $1
            WHERE
                t1.id = ANY($2)""",
        user_id, contacts_ids
    )
    result = {}
    for item in data:
        allow_contact = False
        if item['contact']:
            allow_contact = True
        else:
            if user_status == 'золотой':
                allow_contact = True
            elif user_status == 'серебряный' and item['status'] != 'золотой':
                allow_contact = True
            elif user_status == 'бронзовый' and item['status'] == 'бронзовый':
                allow_contact = True
        result[str(item['user_id'])] = {
            'contact': item['contact'],
            'allow_contact': allow_contact,
        }
    return result



################################################################
async def get_community_managers():
    api = get_api_context()
    data = await api.pg.club.fetch(
        """SELECT
                t1.id, t1.name
            FROM
                users t1
            INNER JOIN
                users_roles t2 ON t2.user_id = t1.id
            INNER JOIN
                roles t3 ON t3.id = t2.role_id
            WHERE
                t3.alias = 'community manager'
            ORDER BY
                t1.name""",
    )
    return [
        { 'id': item['id'], 'name': item['name'] } for item in data
    ]



################################################################
async def get_telegram_pin(user):
    api = get_api_context()
    await api.redis.data.acquire()
    check = 1
    while check != 0:
        pin = ''.join(choice(string.digits) for _ in range(6))
        check = await api.redis.data.exec('EXISTS', '__TELEGRAM__' + pin)
    await api.redis.data.exec('SET', '__TELEGRAM__' + pin, user.id, ex = 900)
    print('SET PIN:', '__TELEGRAM__' + pin)
    api.redis.data.release()
    return pin



################################################################
async def get_last_activity(users_ids):
    api = get_api_context()
    data = await api.pg.club.fetch(
        """SELECT
                user_id, max(time_last_activity) AS time_last_activity
            FROM
                sessions
            WHERE
                user_id = ANY($1)
            GROUP BY
                user_id""",
        users_ids
    )
    return {
        str(item['user_id']): item['time_last_activity'] for item in data
    }



################################################################
async def get_users_memberships(users_ids):
    api = get_api_context()
    data = await api.pg.club.fetch(
        """SELECT
                t1.id,
                COALESCE(t4.polls_count, 0) AS polls_count,
                COALESCE(t5.events_count, 0) AS events_count,
                t2.review, t3.name AS review_author, t2.time_review AS review_time, t2.rating_id
            FROM
                users t1
            INNER JOIN
                (
                    SELECT user_id, count(poll_id) AS polls_count FROM polls_votes GROUP BY user_id
                ) t4 ON t4.user_id = t1.id
            INNER JOIN
                (
                    SELECT user_id, count(event_id) AS events_count FROM events_users WHERE confirmation IS TRUE GROUP BY user_id
                ) t5 ON t5.user_id = t1.id
            LEFT JOIN
                users_managers_reviews t2 ON t2.user_id = t1.id
            LEFT JOIN
                users t3 ON t3.id = t2.author_id
            WHERE
                t1.id = ANY($1)""",
        users_ids
    )




    return {
        str(item['user_id']): item['time_last_activity'] for item in data
    }




    let userMembership = {
        stage: 3,
        semaphore: [
            {
                id: 1,
                name: 'Оценка менеджера',
                rating: 1,
                data: {
                    comment: {
                        text: 'Мой комментарий такой',
                        author: 'Иванов Иван',
                        time: Date.now(),
                    },
                },
            },
            {
                id: 2,
                name: 'Участие в опросах',
                rating: 3,
                data: {
                    value: 0,
                },
                rejection: false,
            },
            {
                id: 3,
                name: 'Участие в мероприятиях',
                rating: 2,
                data: {
                    value: 4,
                },
                rejection: false,
            },
        ],
        stages: [
            {
                id: 1,
                time: null,
                data: {
                    comment: null,
                },
                rejection: false,
            },
            {
                id: 2,
                time: null,
                data: {
                    comment: null,
                },
                rejection: false,
            },
            {
                id: 3,
                time: 1689884260000,
                data: {
                    comment: {
                        text: 'Мой комментарий такой',
                        author: 'Иванов Иван',
                        time: Date.now(),
                    },
                },
                rejection: false,
            },
            {
                id: 4,
                time: null,
                data: {
                    comment: null,
                },
                rejection: false,
            },
            {
                id: 5,
                time: null,
                data: {
                    comment: null,
                },
                rejection: false,
            },
            {
                id: 6,
                time: null,
                data: {
                    comment: null,
                },
                rejection: false,
            },
        ]
    };