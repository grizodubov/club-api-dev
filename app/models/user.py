import re
import math
from random import randint, choice
import string
import os.path
from datetime import datetime
import pytz
import asyncio
import pprint

from app.core.context import get_api_context
from app.utils.packager import pack as data_pack, unpack as data_unpack
from app.models.role import get_roles
from app.models.message import check_recepient, check_recepients
from app.models.notification_1 import get_stats



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
        self.inn = ''
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
        self.score = 0
        self.roles = []
        self._password = ''
        self.avatar_hash = None
        self.online = False
        self.community_manager_id = 0
        self.agent_id = 0
        self.curator_id = 0
        self.curators = None
        self.agent_name = ''
        self.link_telegram = ''
        self.id_telegram = None
        self.tags_1_company_scope = ''
        self.tags_1_company_needs = ''
        self.tags_1_personal_expertise = ''
        self.tags_1_personal_needs = ''
        self.tags_1_licenses = ''
        self.tags_1_hobbies = ''


    ################################################################
    @classmethod
    async def search(cls, text, active_only = True, offset = None, limit = None, count = False, applicant = False, reverse = False, target = None, ids = []):
        api = get_api_context()
        result = []
        amount = None
        slice_query = ''
        conditions = [ 't1.id >= 10000' ]
        condition_query = ''
        args = []
        if ids:
            conditions.append('t1.id = ANY($1)')
            args.append(ids)
        if active_only:
            conditions.append('t1.active IS TRUE')
        if applicant is None:
            applicant = False
        if applicant is True:
            conditions.append("""'applicant' = ANY(t4.roles)""")
        if applicant is False:
            conditions.append("""'applicant' <> ANY(t4.roles)""")
        text = text.strip()
        if text:
            if target:
                if target == 'tags':
                    conditions.append("""regexp_split_to_array($1::text, '\s*,\s*') && regexp_split_to_array(t2.tags, '\s*,\s*')""")
                else:
                    conditions.append("""regexp_split_to_array($1::text, '\s*,\s*') && regexp_split_to_array(t2.interests, '\s*,\s*')""")
                args.append(str(text))
            else:
                if reverse:
                    conditions.append("""(to_tsvector(concat_ws(' ', t1.name, t1.email, t1.phone, t3.company, t3.position, t2.interests)) @@ to_tsquery($1) OR t1.name ILIKE concat('%', $2::text, '%') OR t3.company ILIKE concat('%', $2::text, '%'))""")
                else:
                    conditions.append("""(to_tsvector(concat_ws(' ', t1.name, t1.email, t1.phone, t3.company, t3.position, t3.detail, t2.tags, t2.interests)) @@ to_tsquery($1) OR t1.name ILIKE concat('%', $2::text, '%') OR t3.company ILIKE concat('%', $2::text, '%'))""")
                args.append(re.sub(r'\s+', ' | ', text))
                args.append(str(text))
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
                    t1.agent_id,
                    t1.score,
                    t1.curator_id,
                    cu.curators,
                    coalesce(t9.name, '') AS agent_name,
                    t3.company, t3.position, t3.inn, t3.detail,
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
                    t1.password AS _password,
                    coalesce(ut1.tags, '') AS tags_1_company_scope,
                    coalesce(ut2.tags, '') AS tags_1_company_needs,
                    coalesce(ut3.tags, '') AS tags_1_personal_expertise,
                    coalesce(ut4.tags, '') AS tags_1_personal_needs,
                    coalesce(ut5.tags, '') AS tags_1_licenses,
                    coalesce(ut6.tags, '') AS tags_1_hobbies
                FROM
                    users t1
                INNER JOIN
                    users_tags t2 ON t2.user_id = t1.id
                INNER JOIN
                    users_info t3 ON t3.user_id = t1.id
                LEFT JOIN
                    curators cu ON cu.id = t1.agent_id
                LEFT JOIN
                    users t9 ON t9.id = t1.agent_id
                LEFT JOIN
                    users_tags_1 ut1 ON ut1.user_id = t1.id AND ut1.category = 'company scope'
                LEFT JOIN
                    users_tags_1 ut2 ON ut2.user_id = t1.id AND ut2.category = 'company needs'
                LEFT JOIN
                    users_tags_1 ut3 ON ut3.user_id = t1.id AND ut3.category = 'personal expertise'
                LEFT JOIN
                    users_tags_1 ut4 ON ut4.user_id = t1.id AND ut4.category = 'personal needs'
                LEFT JOIN
                    users_tags_1 ut5 ON ut5.user_id = t1.id AND ut5.category = 'licenses'
                LEFT JOIN
                    users_tags_1 ut6 ON ut6.user_id = t1.id AND ut6.category = 'hobbies'
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
    async def client_search(cls, text, ids = [], community_manager_id = None, agent_id = None, active_only = True, offset = None, limit = None, count = False, applicant = False, reverse = False, target = None, inn = False):
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
        text = text.strip()
        if text:
            inn_query = ''
            if inn:
                inn_query = """ OR t3.inn ILIKE concat('%', $2::text, '%')"""
            if target:
                if target == 'tags':
                    conditions.append("""regexp_split_to_array($1::text, '\s*,\s*') && regexp_split_to_array(t2.tags, '\s*,\s*')""")
                else:
                    conditions.append("""regexp_split_to_array($1::text, '\s*,\s*') && regexp_split_to_array(t2.interests, '\s*,\s*')""")
                args.append(str(text))
            else:
                if reverse:
                    conditions.append("""(to_tsvector(concat_ws(' ', t1.name, t1.email, t1.phone, t3.company, t3.position, t2.interests)) @@ to_tsquery($1) OR t1.name ILIKE concat('%', $2::text, '%') OR t3.company ILIKE concat('%', $2::text, '%'))""" + inn_query)
                else:
                    conditions.append("""(to_tsvector(concat_ws(' ', t1.name, t1.email, t1.phone, t3.company, t3.position, t3.detail, t2.tags, t2.interests)) @@ to_tsquery($1) OR t1.name ILIKE concat('%', $2::text, '%') OR t3.company ILIKE concat('%', $2::text, '%'))""" + inn_query)
                args.append(re.sub(r'\s+', ' | ', text))
                args.append(str(text))
        if ids:
            conditions.append("""t1.id = ANY($""" + str(len(args) + 1) + """)""")
            args.append(ids)
        if community_manager_id and agent_id:
            conditions.append("""(t1.community_manager_id = $""" + str(len(args) + 1) + """ OR t1.agent_id = ANY($""" + str(len(args) + 2) + """))""")
            args.append(community_manager_id)
            args.append(agent_id)
        else:
            if community_manager_id:
                conditions.append("""t1.community_manager_id = $""" + str(len(args) + 1))
                args.append(community_manager_id)
            if agent_id:
                conditions.append("""t1.agent_id = ANY($""" + str(len(args) + 1) + """)""")
                args.append(agent_id)
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
                    t1.agent_id,
                    t1.score,
                    t1.curator_id,
                    cu.curators,
                    coalesce(t9.name, '') AS agent_name,
                    t3.company, t3.position, t3.inn, t3.detail,
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
                    t1.password AS _password,
                    coalesce(ut1.tags, '') AS tags_1_company_scope,
                    coalesce(ut2.tags, '') AS tags_1_company_needs,
                    coalesce(ut3.tags, '') AS tags_1_personal_expertise,
                    coalesce(ut4.tags, '') AS tags_1_personal_needs,
                    coalesce(ut5.tags, '') AS tags_1_licenses,
                    coalesce(ut6.tags, '') AS tags_1_hobbies
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
                    curators cu ON cu.id = t1.agent_id
                LEFT JOIN
                    users t9 ON t9.id = t1.agent_id
                LEFT JOIN
                    users_tags_1 ut1 ON ut1.user_id = t1.id AND ut1.category = 'company scope'
                LEFT JOIN
                    users_tags_1 ut2 ON ut2.user_id = t1.id AND ut2.category = 'company needs'
                LEFT JOIN
                    users_tags_1 ut3 ON ut3.user_id = t1.id AND ut3.category = 'personal expertise'
                LEFT JOIN
                    users_tags_1 ut4 ON ut4.user_id = t1.id AND ut4.category = 'personal needs'
                LEFT JOIN
                    users_tags_1 ut5 ON ut5.user_id = t1.id AND ut5.category = 'licenses'
                LEFT JOIN
                    users_tags_1 ut6 ON ut6.user_id = t1.id AND ut6.category = 'hobbies'
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
                            ) t4 ON t4.user_id = t1.id AND 'client'::text = ANY(t4.roles)""" + conditions_query,
                    *args_count
                )
            return (result, amount)
        return result


    ################################################################
    @classmethod
    async def agent_search(cls, text, ids = [], community_manager_id = None, active_only = True, offset = None, limit = None, count = False, applicant = False, reverse = False, target = None):
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
        text = text.strip()
        if text:
            if target:
                if target == 'tags':
                    conditions.append("""regexp_split_to_array($1::text, '\s*,\s*') && regexp_split_to_array(t2.tags, '\s*,\s*')""")
                else:
                    conditions.append("""regexp_split_to_array($1::text, '\s*,\s*') && regexp_split_to_array(t2.interests, '\s*,\s*')""")
                args.append(str(text))
            else:
                if reverse:
                    conditions.append("""(to_tsvector(concat_ws(' ', t1.name, t1.email, t1.phone, t3.company, t3.position, t2.interests)) @@ to_tsquery($1) OR t1.name ILIKE concat('%', $2::text, '%') OR t3.company ILIKE concat('%', $2::text, '%'))""")
                else:
                    conditions.append("""(to_tsvector(concat_ws(' ', t1.name, t1.email, t1.phone, t3.company, t3.position, t3.detail, t2.tags, t2.interests)) @@ to_tsquery($1) OR t1.name ILIKE concat('%', $2::text, '%') OR t3.company ILIKE concat('%', $2::text, '%'))""")
                args.append(re.sub(r'\s+', ' | ', text))
                args.append(str(text))
        if ids:
            conditions.append("""t1.id = ANY($""" + str(len(args) + 1) + """)""")
            args.append(ids)
        if community_manager_id:
            conditions.append("""t1.community_manager_id = $""" + str(len(args) + 1))
            args.append(community_manager_id)
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
                    t1.agent_id,
                    t1.score,
                    t1.curator_id,
                    cu.curators,
                    coalesce(t9.name, '') AS agent_name,
                    t3.company, t3.position, t3.inn, t3.detail,
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
                    t1.password AS _password,
                    coalesce(ut1.tags, '') AS tags_1_company_scope,
                    coalesce(ut2.tags, '') AS tags_1_company_needs,
                    coalesce(ut3.tags, '') AS tags_1_personal_expertise,
                    coalesce(ut4.tags, '') AS tags_1_personal_needs,
                    coalesce(ut5.tags, '') AS tags_1_licenses,
                    coalesce(ut6.tags, '') AS tags_1_hobbies
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
                    ) t4 ON t4.user_id = t1.id
                LEFT JOIN
                    curators cu ON cu.id = t1.agent_id
                LEFT JOIN
                    users t9 ON t9.id = t1.agent_id
                LEFT JOIN
                    users_tags_1 ut1 ON ut1.user_id = t1.id AND ut1.category = 'company scope'
                LEFT JOIN
                    users_tags_1 ut2 ON ut2.user_id = t1.id AND ut2.category = 'company needs'
                LEFT JOIN
                    users_tags_1 ut3 ON ut3.user_id = t1.id AND ut3.category = 'personal expertise'
                LEFT JOIN
                    users_tags_1 ut4 ON ut4.user_id = t1.id AND ut4.category = 'personal needs'
                LEFT JOIN
                    users_tags_1 ut5 ON ut5.user_id = t1.id AND ut5.category = 'licenses'
                LEFT JOIN
                    users_tags_1 ut6 ON ut6.user_id = t1.id AND ut6.category = 'hobbies'
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
                            ) t4 ON t4.user_id = t1.id AND 'client'::text = ANY(t4.roles)""" + conditions_query,
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
        filter = { 'time_create', 'time_update', 'community_manager_id', 'agent_id', 'score', 'curator_id', 'curators', 'agent_name', 'login', 'email', 'phone', 'inn', 'roles', 'annual', 'annual_privacy', 'employees', 'employees_privacy', 'birthdate', 'birthdate_privacy' }
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
        filter = { 'time_create', 'time_update', 'login', 'email', 'phone', 'inn', 'roles', 'id_telegram' }
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
            if active is not None and active is True:
                query = ' AND t1.active IS TRUE'
            if active is not None and active is False:
                query = ' AND t1.active IS FALSE'
            data = await api.pg.club.fetchrow(
                """SELECT
                        t1.id, t1.time_create, t1.time_update,
                        t1.name, t1.login, t1.email, t1.phone,
                        t1.active,
                        t1.community_manager_id,
                        t1.agent_id,
                        t1.score,
                        t1.curator_id,
                        cu.curators,
                        coalesce(t9.name, '') AS agent_name,
                        t3.company, t3.position, t3.inn, t3.detail,
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
                        t1.password AS _password,
                        coalesce(ut1.tags, '') AS tags_1_company_scope,
                        coalesce(ut2.tags, '') AS tags_1_company_needs,
                        coalesce(ut3.tags, '') AS tags_1_personal_expertise,
                        coalesce(ut4.tags, '') AS tags_1_personal_needs,
                        coalesce(ut5.tags, '') AS tags_1_licenses,
                        coalesce(ut6.tags, '') AS tags_1_hobbies
                    FROM
                        users t1
                    INNER JOIN
                        users_tags t2 ON t2.user_id = t1.id
                    INNER JOIN
                        users_info t3 ON t3.user_id = t1.id
                    LEFT JOIN
                        curators cu ON cu.id = t1.agent_id
                    LEFT JOIN
                        users t9 ON t9.id = t1.agent_id
                    LEFT JOIN
                        users_tags_1 ut1 ON ut1.user_id = t1.id AND ut1.category = 'company scope'
                    LEFT JOIN
                        users_tags_1 ut2 ON ut2.user_id = t1.id AND ut2.category = 'company needs'
                    LEFT JOIN
                        users_tags_1 ut3 ON ut3.user_id = t1.id AND ut3.category = 'personal expertise'
                    LEFT JOIN
                        users_tags_1 ut4 ON ut4.user_id = t1.id AND ut4.category = 'personal needs'
                    LEFT JOIN
                        users_tags_1 ut5 ON ut5.user_id = t1.id AND ut5.category = 'licenses'
                    LEFT JOIN
                        users_tags_1 ut6 ON ut6.user_id = t1.id AND ut6.category = 'hobbies'
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
        for k in { 'active', 'name', 'email', 'phone', 'password', 'community_manager_id', 'agent_id', 'curator_id' }:
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
                    link_telegram = $16,
                    inn = $17
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
            kwargs['link_telegram'] if 'link_telegram' in kwargs else '',
            kwargs['inn'] if 'inn' in kwargs else '',
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
        calls = []
        for tk in { 'company scope', 'company needs', 'personal expertise', 'personal needs', 'licenses', 'hobbies' }:
            ktk = 'tags_1_' + tk.replace(' ', '_')
            if ktk in kwargs:
                tags_old = set(sorted(re.split(r'\s*\+\s*', getattr(self, ktk))))
                tags_new = set(sorted(re.split(r'\s*\+\s*', kwargs[ktk].strip())))
                if tags_old != tags_new:
                    calls_data = {}
                    call_data = ' + '.join([ t for t in re.split(r'\s*\+\s*', kwargs[ktk].strip()) if t ])
                    calls.append(
                        api.pg.club.execute(
                            """INSERT INTO
                                    users_tags_1 (user_id, category, tags)
                                VALUES
                                    ($1, $2, $3)
                                ON CONFLICT
                                    (user_id, category)
                                DO UPDATE SET
                                    tags = EXCLUDED.tags,
                                    time_update = now() at time zone 'utc'""",
                            self.id, tk, call_data
                        )
                    )
        if calls:
            await asyncio.gather(*calls)


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
                    if k == 'phone':
                        # Только мобильные телефоны РФ
                        ar.append('+7' + ''.join(list(re.sub(r'[^\d]+', '', v))[-10:]))
                    else:
                        ar.append(v)
                    rk = k
                    if k in { 'email', 'phone' }:
                        rk = 't1.' + rk
                    qr.append(rk + ' = $' + str(i))
                    i += 1
            if qr:
                data = await api.pg.club.fetchrow(
                    """SELECT
                            t1.id, t1.time_create, t1.time_update,
                            t1.name, t1.login, t1.email, t1.phone,
                            t1.active,
                            t1.community_manager_id,
                            t1.agent_id,
                            t1.score,
                            t1.curator_id,
                            cu.curators,
                            coalesce(t9.name, '') AS agent_name,
                            t3.company, t3.position, t3.inn, t3.detail,
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
                            t1.password AS _password,
                            coalesce(ut1.tags, '') AS tags_1_company_scope,
                            coalesce(ut2.tags, '') AS tags_1_company_needs,
                            coalesce(ut3.tags, '') AS tags_1_personal_expertise,
                            coalesce(ut4.tags, '') AS tags_1_personal_needs,
                            coalesce(ut5.tags, '') AS tags_1_licenses,
                            coalesce(ut6.tags, '') AS tags_1_hobbies
                        FROM
                            users t1
                        INNER JOIN
                            users_tags t2 ON t2.user_id = t1.id
                        INNER JOIN
                            users_info t3 ON t3.user_id = t1.id
                        LEFT JOIN
                            curators cu ON cu.id = t1.agent_id
                        LEFT JOIN
                            users t9 ON t9.id = t1.agent_id
                        LEFT JOIN
                            users_tags_1 ut1 ON ut1.user_id = t1.id AND ut1.category = 'company scope'
                        LEFT JOIN
                            users_tags_1 ut2 ON ut2.user_id = t1.id AND ut2.category = 'company needs'
                        LEFT JOIN
                            users_tags_1 ut3 ON ut3.user_id = t1.id AND ut3.category = 'personal expertise'
                        LEFT JOIN
                            users_tags_1 ut4 ON ut4.user_id = t1.id AND ut4.category = 'personal needs'
                        LEFT JOIN
                            users_tags_1 ut5 ON ut5.user_id = t1.id AND ut5.category = 'licenses'
                        LEFT JOIN
                            users_tags_1 ut6 ON ut6.user_id = t1.id AND ut6.category = 'hobbies'
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
                        WHERE """ + ' AND '.join(qr),
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
                        t1.agent_id,
                        t1.score,
                        t1.curator_id,
                        cu.curators,
                        coalesce(t9.name, '') AS agent_name,
                        t3.company, t3.position, t3.inn, t3.detail,
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
                        t1.password AS _password,
                        coalesce(ut1.tags, '') AS tags_1_company_scope,
                        coalesce(ut2.tags, '') AS tags_1_company_needs,
                        coalesce(ut3.tags, '') AS tags_1_personal_expertise,
                        coalesce(ut4.tags, '') AS tags_1_personal_needs,
                        coalesce(ut5.tags, '') AS tags_1_licenses,
                        coalesce(ut6.tags, '') AS tags_1_hobbies
                    FROM
                        users t1
                    INNER JOIN
                        users_tags t2 ON t2.user_id = t1.id
                    INNER JOIN
                        users_info t3 ON t3.user_id = t1.id
                    LEFT JOIN
                        curators cu ON cu.id = t1.agent_id
                    LEFT JOIN
                        users t9 ON t9.id = t1.agent_id
                    LEFT JOIN
                        users_tags_1 ut1 ON ut1.user_id = t1.id AND ut1.category = 'company scope'
                    LEFT JOIN
                        users_tags_1 ut2 ON ut2.user_id = t1.id AND ut2.category = 'company needs'
                    LEFT JOIN
                        users_tags_1 ut3 ON ut3.user_id = t1.id AND ut3.category = 'personal expertise'
                    LEFT JOIN
                        users_tags_1 ut4 ON ut4.user_id = t1.id AND ut4.category = 'personal needs'
                    LEFT JOIN
                        users_tags_1 ut5 ON ut5.user_id = t1.id AND ut5.category = 'licenses'
                    LEFT JOIN
                        users_tags_1 ut6 ON ut6.user_id = t1.id AND ut6.category = 'hobbies'
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
                        (t1.login = $1 OR t1.email = $1 OR t1.phone = $2) AND
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
    async def set_change_code(self, type, code):
        api = get_api_context()
        k = '_AUTH_' + type + '_' + str(self.id) + '_' + code
        await api.redis.data.exec('SET', k, 1, ex = 300)


    ################################################################
    async def check_change_code(self, type, code):
        api = get_api_context()
        k = '_AUTH_' + type + '_' + str(self.id) + '_' + code
        check = await api.redis.data.exec('GET', k)
        # print('CHECK', check)
        if check:
            await api.redis.data.exec('DELETE', k)
            return True
        return False


    ################################################################
    async def update_email(self, email):
        api = get_api_context()
        await api.pg.club.execute(
            """UPDATE users SET email = $1 WHERE id = $2""",
            email, self.id
        )


    ################################################################
    async def update_phone(self, phone):
        api = get_api_context()
        await api.pg.club.execute(
            """UPDATE users SET phone = $1 WHERE id = $2""",
            phone, self.id
        )


    ################################################################
    async def update_password(self, password):
        api = get_api_context()
        await api.pg.club.execute(
            """UPDATE users SET password = $1 WHERE id = $2""",
            password, self.id
        )


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
                            SELECT id FROM events WHERE time_event >= (now() at time zone 'utc')::date AND active IS TRUE
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
                    t4.company, t4.position, t4.status, t4.catalog,
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
                    NULL AS company, NULL AS position, NULL AS status, NULL AS catalog,
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
    async def get_event_suggestions(self, event_id, users_ids):
        api = get_api_context()
        if users_ids:
            self_tags = await api.pg.club.fetchrow(
                """SELECT coalesce(tags, '') AS tags, coalesce(interests, '') AS interests FROM users_events_tags WHERE user_id = $1 AND event_id = $2""",
                self.id, event_id
            )
            if not self_tags:
                self_tags = { 'tags': '', 'interests': '' }
            query_tags = """SELECT
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
                                    (
                                        SELECT
                                            s1.user_id, (coalesce(s1.tags, '') || ',' || coalesce(s2.tags, '')) AS tags
                                        FROM
                                            users_tags s1
                                        LEFT JOIN
                                            users_events_tags s2 ON s2.user_id = s1.user_id AND s2.event_id = $4
                                    ) t2 ON t2.user_id = t1.id
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
                                    t1.id = ANY($3) AND
                                    t1.active IS TRUE AND
                                    to_tsvector(t2.tags) @@ to_tsquery($1)"""
            query_interests = """SELECT
                                        t1.id, t1.name,
                                        t3.company, t3.position, t3.status,
                                        t3.link_telegram,
                                        ts_headline(t2.interests, to_tsquery($2), 'HighlightAll=true, StartSel=~, StopSel=~') AS tags,
                                        $2 AS search,
                                        'ask' AS offer,
                                        t8.hash AS avatar_hash,
                                        ts_rank_cd(to_tsvector(t2.interests), to_tsquery($2), 32) AS __rank
                                    FROM
                                        users t1
                                    INNER JOIN
                                        (
                                            SELECT
                                                s1.user_id, (coalesce(s1.interests, '') || ',' || coalesce(s2.interests, '')) AS interests
                                            FROM
                                                users_tags s1
                                            LEFT JOIN
                                                users_events_tags s2 ON s2.user_id = s1.user_id AND s2.event_id = $4
                                        ) t2 ON t2.user_id = t1.id
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
                                        t1.id = ANY($3) AND
                                        t1.active IS TRUE AND
                                        to_tsvector(t2.interests) @@ to_tsquery(${i})"""
            i = 1
            query_parts = []
            args = []
            query1 = ' | '.join([ re.sub(r'\s+', ' & ', t.strip()) for t in (self.interests + ',' + self_tags['interests']).split(',') if t ])
            #print(query1)
            query2 = ' | '.join([ re.sub(r'\s+', ' & ', t.strip()) for t in (self.tags + ',' + self_tags['tags']).split(',') if t ])
            #print(query2)
            query_parts.extend([ query_tags.format(i = 1), query_interests.format(i = 2) ])
            args.extend([ query1, query2, users_ids, event_id ])
            i = 5
            query_condition = """u.id <> ${i} AND
                        u.id NOT IN (
                            SELECT contact_id FROM users_contacts WHERE user_id = ${i}
                        )"""
            query_condition = query_condition.format(i = i)
            args.append(self.id)
            i += 1
            data = await api.pg.club.fetch(
                """SELECT
                        id, name, company, position, status, link_telegram, tags, search, offer, avatar_hash, t5.amount AS helpful, count(*) OVER() AS amount
                    FROM
                        (
                            SELECT * FROM
                                (""" + ' UNION ALL '.join(query_parts) + """
                                ) d
                        ) u
                    LEFT JOIN
                    (
                        SELECT author_id, count(id) AS amount FROM posts WHERE helpful IS TRUE GROUP BY author_id
                    ) t5 ON t5.author_id = u.id
                    WHERE """ + query_condition,
                *args
            )
            return [ dict(item) | { 'online': check_online_by_id(item['id']) } for item in data ]
        return []


    ################################################################
    async def get_suggestions_new(self, filter = None, from_id = None, users_ids = None, today_offset = None, no_favorites = True, no_contacts = True):
        api = get_api_context()
        if filter is None:
            filter = [ 'company scope', 'company needs' ]
        offers = {
            'company scope': 'bid',
            'company needs': 'ask',
        }
        mapping = {
            'company scope': 'company needs',
            'company needs': 'company scope',
        }
        mask = [ 'company scope', 'company needs' ]
        filter = [ f for f in filter if f in mask ]
        temp_ids = None
        if no_favorites:
            temp_ids = await api.pg.club.fetchval(
                """SELECT
                        array_agg(target_id)
                    FROM
                        users_favorites
                    WHERE
                        user_id = $1 AND flag = false""",
                self.id
            )
        ids = [ self.id ]
        if temp_ids:
            for temp_id in temp_ids:
                ids.append(temp_id)
        if filter:
            queries = []
            args = []
            for f in filter:
                p = getattr(self, 'tags_1_' + re.sub(r'\s', '_', mapping[f]))
                if p.strip():
                    query = """SELECT
                                    t1.id, t1.name, t2.time_update AS time_create, t1.active,
                                    t3.company, t3.position, t3.status, t3.catalog,
                                    t3.link_telegram,
                                    t2.tags AS tags,
                                    ${v2} AS offer,
                                    t8.hash AS avatar_hash,
                                    cm.id AS community_manager_id,
                                    cm.name AS community_manager
                                FROM
                                    users t1
                                INNER JOIN
                                    users_tags_1 t2 ON t2.user_id = t1.id AND t2.category = ${v3}
                                INNER JOIN
                                    users_info t3 ON t3.user_id = t1.id
                                LEFT JOIN
                                    users cm ON cm.id = t1.community_manager_id
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
                                    NOT t1.id = ANY(${v4}) AND
                                    t2.tags SIMILAR TO ${v1} ESCAPE '#'""".format(v1 = len(args) + 1, v2 = len(args) + 2, v3 = len(args) + 3, v4 = len(args) + 4)
                    queries.append(query)
                    args.append('%(' + '|'.join([ re.sub(r'\|', '#|', t.strip()) for t in p.strip().split('+') if t.strip() ]) + ')%')
                    args.append(offers[f])
                    args.append(f)
                    args.append(ids)
            if queries:
                where = []
                query_where = ''
                #
                where.append('id <> ${v} '.format(v = len(args) + 1))
                args.append(self.id)
                #
                if no_contacts:
                    where.append('id NOT IN (SELECT contact_id FROM users_contacts WHERE user_id = ${v})'.format(v = len(args) + 1))
                    args.append(self.id)
                # from_id
                if from_id:
                    where.append('id > ${v} '.format(v = len(args) + 1))
                    args.append(id)
                # users_ids
                if users_ids:
                    where.append('id = ANY(${v})'.format(v = len(args) + 1))
                    args.append(users_ids)
                # today_offset
                if today_offset:
                    where.append('time_create >= (now() at time zone \'utc\')::date + (${v} * interval \'1 minute\')'.format(v = len(args) + 1))
                    args.append(today_offset)
                if where:
                    query_where = ' WHERE ' + ' AND '.join(where)
                args.append(self.id)
                data = await api.pg.club.fetch(
                    """SELECT
                            id, name, active, time_create, company, position, status, catalog, link_telegram, tags,
                            community_manager_id, community_manager, t9.state,
                            offer, avatar_hash, t5.amount AS helpful, ct.comments, count(*) OVER() AS amount
                        FROM
                            (
                                SELECT * FROM
                                    (""" + ' UNION ALL '.join(queries) + """
                                    ) d""" + query_where + """
                                ORDER BY
                                    time_create DESC
                            ) u
                        LEFT JOIN
                        (
                            SELECT author_id, count(id) AS amount FROM posts WHERE helpful IS TRUE GROUP BY author_id
                        ) t5 ON t5.author_id = u.id
                        LEFT JOIN
                            users_suggestions t9 ON t9.user_id = $""" + str(len(args)) + """ AND t9.partner_id = u.id
                        LEFT JOIN
                        (
                            SELECT
                                ct7.partner_id, array_agg(comment) AS comments
                            FROM (
                                SELECT
                                    ct5.user_id, ct5.partner_id, jsonb_build_object(
                                        'id', ct5.id,
                                        'time_create', ct5.time_create,
                                        'comment', ct5.comment,
                                        'author_id', ct5.author_id,
                                        'author_name', ct6.name
                                    ) AS comment
                                FROM
                                    suggestions_comments ct5
                                INNER JOIN
                                    users ct6 ON ct6.id = ct5.author_id
                                WHERE
                                    ct5.user_id = $""" + str(len(args)) + """
                                ORDER BY ct5.id DESC
                            ) ct7
                            GROUP BY ct7.partner_id
                        ) ct ON ct.partner_id = u.id""",
                    *args
                )
                return [ dict(item) | { 'online': check_online_by_id(item['id']) } for item in data ]
        return []


    ################################################################
    async def get_suggestions_new_for_swiper(self, filter = None):
        api = get_api_context()
        if filter is None:
            filter = [ 'company scope', 'company needs' ]
        offers = {
            'company scope': 'bid',
            'company needs': 'ask',
        }
        mapping = {
            'company scope': 'company needs',
            'company needs': 'company scope',
        }
        mask = [ 'company scope', 'company needs' ]
        filter = [ f for f in filter if f in mask ]
        if filter:
            queries = []
            args = []
            for f in filter:
                p = getattr(self, 'tags_1_' + re.sub(r'\s', '_', mapping[f]))
                if p.strip():
                    query = """SELECT
                                    t1.id, t1.name, t2.time_update AS time_create,
                                    t3.company, t3.position, t3.catalog,
                                    coalesce(h1.tags, '') AS hobby,
                                    t2.tags AS tags,
                                    ${v2} AS offer,
                                    t8.hash AS avatar_hash,
                                    cm.id AS community_manager_id,
                                    cm.name AS community_manager
                                FROM
                                    users t1
                                INNER JOIN
                                    users_tags_1 t2 ON t2.user_id = t1.id AND t2.category = ${v3}
                                INNER JOIN
                                    users_info t3 ON t3.user_id = t1.id
                                INNER JOIN
                                    users_roles r1 ON r1.user_id = t1.id
                                INNER JOIN
                                    roles r2 ON r2.id = r1.role_id AND r2.alias = 'client'
                                LEFT JOIN
                                    users_tags_1 h1 ON h1.user_id = t1.id AND h1.category = 'hobbies'
                                LEFT JOIN
                                    users cm ON cm.id = t1.community_manager_id
                                LEFT JOIN
                                    avatars t8 ON t8.owner_id = t1.id AND t8.active IS TRUE
                                WHERE
                                    t1.id >= 10000 AND
                                    t1.id NOT IN (
                                        SELECT target_id FROM users_favorites WHERE user_id = ${v4}
                                    ) AND
                                    t1.active IS TRUE AND
                                    t2.tags SIMILAR TO ${v1} ESCAPE '#'""".format(v1 = len(args) + 1, v2 = len(args) + 2, v3 = len(args) + 3, v4 = len(args) + 4)
                    queries.append(query)
                    args.append('%(' + '|'.join([ re.sub(r'\|', '#|', t.strip()) for t in p.strip().split('+') if t.strip() ]) + ')%')
                    args.append(offers[f])
                    args.append(f)
                    args.append(self.id)
            if queries:
                where = []
                query_where = ''
                #
                where.append('id <> ${v} '.format(v = len(args) + 1))
                args.append(self.id)
                #
                where.append('id NOT IN (SELECT contact_id FROM users_contacts WHERE user_id = ${v})'.format(v = len(args) + 1))
                args.append(self.id)
                if where:
                    query_where = ' WHERE ' + ' AND '.join(where)
                data = await api.pg.club.fetch(
                    """SELECT
                            id, name, time_create, company, position, catalog, hobby, tags,
                            community_manager_id, community_manager,
                            offer, avatar_hash
                        FROM
                            (
                                SELECT * FROM
                                    (""" + ' UNION ALL '.join(queries) + """
                                    ) d""" + query_where + """
                                ORDER BY
                                    time_create DESC
                            ) u
                        ORDER BY
                            id
                        LIMIT
                            20
                        """,
                    *args
                )
                if data:
                    result = []
                    cache = {}
                    for item in data: 
                        temp = {}
                        if item['offer'] == 'bid':
                            ps = [ t.strip() for t in self.tags_1_company_needs.strip().split('+') if t.strip() ]
                            pt = [ t.strip() for t in item['tags'].strip().split('+') if t.strip() ]
                            pi = [ v for v in ps if v in pt ]
                            if pi:
                                temp['company scope'] = pt
                                temp['company scope intersections'] = pi
                        elif item['offer'] == 'ask':
                            ps = [ t.strip() for t in self.tags_1_company_scope.strip().split('+') if t.strip() ]
                            pt = [ t.strip() for t in item['tags'].strip().split('+') if t.strip() ]
                            pi = [ v for v in ps if v in pt ]
                            if pi:
                                temp['company needs'] = pt
                                temp['company needs intersections'] = pi
                        if temp:
                            if str(item['id']) in cache:
                                i = cache[str(item['id'])]
                                result[i]['tags'] = result[i]['tags'] | temp
                                result[i]['time_update'] = result[i]['time_update'] if result[i]['time_update'] > item['time_create'] else item['time_create']
                                result[i]['offer'].append(item['offer'])
                            else:
                                result.append({
                                    'id': item['id'],
                                    'name': item['name'],
                                    'time_update': item['time_create'],
                                    'company': item['company'],
                                    'position': item['position'],
                                    'catalog': [ t.strip() for t in item['catalog'].strip().split(',') if t.strip() ],
                                    'hobby': [ t.strip() for t in item['hobby'].strip().split('+') if t.strip() ],
                                    'tags': temp,
                                    'community_manager_id': item['community_manager_id'],
                                    'community_manager': item['community_manager'],
                                    'offer': [ item['offer'] ],
                                    'avatar_hash': item['avatar_hash'],
                                    'online': check_online_by_id(item['id']),
                                })
                                cache[str(item['id'])] = len(result) - 1
                    for item in result:
                        # company tags
                        l = 0
                        is_tag = {
                            'company scope': True,
                            'company needs': True,
                        }
                        current_index = 0
                        s = {
                            'company scope': [],
                            'company needs': [],
                        }
                        while is_tag['company scope'] or is_tag['company needs']:
                            for k in { 'company scope', 'company needs' }:
                                if is_tag[k]:
                                    if k + ' intersections' in item['tags']:
                                        lst = item['tags'][k + ' intersections']
                                        if (len(lst) > current_index):
                                            pl = [ pp.strip() for pp in lst[current_index].split('|') if pp.strip()][-1]
                                            l += len(pl)
                                            if l > 144:
                                                if len(s[k]) < 1:
                                                    s[k].append(pl)
                                                    if len(lst) > current_index + 1:
                                                        s[k].append('...')
                                                else:
                                                    s[k].append('...')
                                                is_tag[k] = False
                                            else:
                                                s[k].append(pl)
                                        else:
                                            is_tag[k] = False
                                    else:
                                        is_tag[k] = False
                            current_index += 1
                        if s['company scope']:
                            item['tags']['company scope intersections filtered'] = s['company scope']
                        if s['company needs']:
                            item['tags']['company needs intersections filtered'] = s['company needs']
                        # catalog, hobby
                        for k in { 'catalog', 'hobby' }:
                            if item[k]:
                                s = []
                                l = 0
                                i = 0
                                for p in item[k]:
                                    pl = [ pp.strip() for pp in p.split('|') if pp.strip()][-1]
                                    l += len(pl)
                                    if l > 380:
                                        if len(s) < 1:
                                            s.append(pl)
                                            if len(item[k]) > i + 1:
                                                s.append('...')
                                        else:
                                            s.append('...')
                                        break
                                    s.append(pl)
                                    i += 1
                                item[k + ' filtered'] = s
                    return result
        return []


    ################################################################
    async def get_events_summary(self):
        api = get_api_context()
        result = []
        events = await api.pg.club.fetch(
            """SELECT
                    t1.id, t1.name, t1.format, t1.place, t1.time_event, t1.detail,
                    t2.confirmation, t2.audit, t2.guests
                FROM
                    events t1
                INNER JOIN
                    events_users t2 ON t2.event_id = t1.id AND t2.user_id = $1
                WHERE
                    t1.active IS TRUE
                ORDER BY
                    t1.time_event DESC""",
            self.id
        )
        stats = {
            'events': [ 0, 0, 0 ],
            'connections': [ 0, 0, 0 ],
        }
        if events:
            connections = await self.get_connections_summary()
            for event in events:
                temp = dict(event)
                if 'connections' not in temp:
                    temp.update(
                        {
                            'connections': [],
                            'stats': [ 0, 0, 0 ],
                        }
                    )
                stats['events'][0] += 1
                if temp['audit'] == 2:
                    stats['events'][2] += 1
                else:
                    stats['events'][1] += 1
                for connection in connections:
                    if connection['event_id'] == temp['id']:
                        temp['connections'].append(connection)
                        stats['connections'][0] += 1
                        temp['stats'][0] += 1
                        if connection['state']:
                            stats['connections'][2] += 1
                            temp['stats'][2] += 1
                        else:
                            stats['connections'][1] += 1
                            temp['stats'][1] += 1
                result.append(temp)
        return {
            'events': result,
            'stats': stats,
        }


    ################################################################
    async def get_connections_summary(self):
        api = get_api_context()
        data = await api.pg.club.fetch(
            """SELECT
                    t1.id, t1.event_id, t1.user_1_id, t1.user_2_id, t1.state, t1.rating_1, t1.rating_2,
                    t1.response,
                    t2.name AS user_1_name, t3.name AS user_2_name,
                    t2.active AS user_1_active, t3.active AS user_2_active,
                    t1.creator_id, t4.name AS creator, t1.deleted,
                    t22.id AS community_manager_1_id, t33.id AS community_manager_2_id, 
                    coalesce(t22.name, '') AS community_manager_1,
                    coalesce(t33.name, '') AS community_manager_2,
                    t7.hash AS user_1_avatar_hash,
                    t8.hash AS user_2_avatar_hash,
                    t222.company AS user_1_company,
                    t333.company AS user_2_company,
                    ct.comments
                FROM
                    users_connections t1
                LEFT JOIN
                    users t2 ON t2.id = t1.user_1_id
                LEFT JOIN
                    users_info t222 ON t222.user_id = t1.user_1_id
                LEFT JOIN
                    users t22 ON t22.id = t2.community_manager_id
                LEFT JOIN
                    users t3 ON t3.id = t1.user_2_id
                LEFT JOIN
                    users_info t333 ON t333.user_id = t1.user_2_id
                LEFT JOIN
                    users t33 ON t33.id = t3.community_manager_id
                LEFT JOIN
                    users t4 ON t4.id = t1.creator_id
                LEFT JOIN
                    avatars t7 ON t7.owner_id = t1.user_1_id AND t7.active IS TRUE
                LEFT JOIN
                    avatars t8 ON t8.owner_id = t1.user_2_id AND t8.active IS TRUE
                LEFT JOIN
                    (
                        SELECT
                            ct7.connection_id, array_agg(comment) AS comments
                        FROM (
                            SELECT
                                ct5.connection_id, jsonb_build_object(
                                    'id', ct5.id,
                                    'time_create', ct5.time_create,
                                    'comment', ct5.comment,
                                    'author_id', ct5.author_id,
                                    'author_name', ct6.name
                                ) AS comment
                            FROM
                                connections_comments ct5
                            INNER JOIN
                                users ct6 ON ct6.id = ct5.author_id
                            ORDER BY ct5.id DESC
                        ) ct7
                        GROUP BY ct7.connection_id
                    ) ct ON ct.connection_id = t1.id
                WHERE
                    t1.deleted IS FALSE AND
                    (t1.user_1_id = $1 OR t1.user_2_id = $1)""",
            self.id
        )
        if data:
            return [ dict(item) for item in data ]
        return []


    ################################################################
    async def add_contact(self, contact_id):
        api = get_api_context()
        await api.pg.club.execute(
            """INSERT INTO users_contacts (user_id, contact_id) VALUES ($1, $2) ON CONFLICT (user_id, contact_id) DO NOTHING""",
            self.id, contact_id
        )


    ################################################################
    async def del_contact(self, contact_id):
        api = get_api_context()
        await api.pg.club.execute(
            """DELETE FROM users_contacts WHERE user_id = $1 AND contact_id = $2""",
            self.id, contact_id
        )


    ################################################################
    async def add_event(self, event_id):
        api = get_api_context()
        await api.pg.club.execute(
            """INSERT INTO events_users (event_id, user_id) VALUES ($1, $2) ON CONFLICT (event_id, user_id) DO NOTHING""",
            event_id, self.id
        )


    ################################################################
    async def check_event(self, event_id):
        api = get_api_context()
        result = await api.pg.club.fetchval(
            """SELECT user_id FROM events_users WHERE event_id = $1 AND user_id = $2""",
            event_id, self.id
        )
        #print(result)
        if result:
            return True
        return False


    ################################################################
    async def audit_event(self, event_id, audit):
        api = get_api_context()
        await api.pg.club.execute(
            """UPDATE events_users SET audit = $3 WHERE event_id = $1 AND user_id = $2""",
            event_id, self.id, audit
        )
    

    ################################################################
    async def guests_event(self, event_id, guests):
        api = get_api_context()
        await api.pg.club.execute(
            """UPDATE events_users SET guests = $3 WHERE event_id = $1 AND user_id = $2""",
            event_id, self.id, guests
        )


    ################################################################
    async def del_event(self, event_id):
        api = get_api_context()
        await api.pg.club.execute(
            """DELETE FROM events_users WHERE event_id = $1 AND user_id = $2""",
            event_id, self.id
        )


    ################################################################
    async def get_events(self):
        api = get_api_context()
        data = await api.pg.club.fetch(
            """SELECT
                    t1.id, t1.name, t1.format, t1.place, t1.time_event, t1.detail,
                    t2.confirmation, t2.audit, t2.guests, FALSE AS archive
                FROM
                    events t1
                LEFT JOIN
                    events_users t2 ON t2.event_id = t1.id AND t2.user_id = $1
                WHERE
                    t1.active IS TRUE AND
                    t1.time_event >= (now() at time zone 'utc')::date
                ORDER BY
                    t1.time_event""",
            self.id
        )
        return [ dict(item) for item in data ]


    ################################################################
    async def get_events_confirmations_pendings(self):
        api = get_api_context()
        amount = await api.pg.club.fetchval(
            """SELECT
                    count(t1.id)
                FROM
                    events t1
                INNER JOIN
                    events_users t2 ON t2.event_id = t1.id AND t2.user_id = $1
                WHERE
                    t1.active IS TRUE AND
                    t1.time_event >= (now() at time zone 'utc')::date AND
                    t2.confirmation IS FALSE""",
            self.id
        )
        return amount


    ################################################################
    async def get_events_archive(self):
        api = get_api_context()
        dt_now = datetime.now(tz = pytz.utc)
        if dt_now.month == 1:
            dt_control = datetime(dt_now.year - 1, 11, 1, tzinfo = pytz.utc)
        elif dt_now.month == 2:
            dt_control = datetime(dt_now.year - 1, 12, 1, tzinfo = pytz.utc)
        else:
            dt_control = datetime(dt_now.year, dt_now.month - 2, 1, tzinfo = pytz.utc)
        data = await api.pg.club.fetch(
            """SELECT
                    t1.id, t1.name, t1.format, t1.place, t1.time_event, t1.detail,
                    t2.confirmation, t2.audit, t2.guests, TRUE AS archive
                FROM
                    events t1
                INNER JOIN
                    events_users t2 ON t2.event_id = t1.id AND t2.user_id = $1
                WHERE
                    t1.active IS TRUE AND
                    t1.time_event < (now() at time zone 'utc')::date AND
                    t1.time_event >= $2 AND
                    t2.confirmation IS TRUE
                ORDER BY
                    t1.time_event DESC""",
            self.id, dt_control.timestamp() * 1000
        )
        return [ dict(item) for item in data ]


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
                    users (name, email, phone, password, active, community_manager_id, agent_id, curator_id)
                VALUES
                    ($1, $2, $3, $4, $5, $6, $7, $8)
                RETURNING
                    id""",
            kwargs['name'],
            kwargs['email'],
            '+7' + ''.join(list(re.sub(r'[^\d]+', '', kwargs['phone']))[-10:]),
            kwargs['password'],
            kwargs['active'] if 'active' in kwargs else True,
            kwargs['community_manager_id'] if 'community_manager_id' in kwargs and kwargs['community_manager_id'] else None,
            kwargs['agent_id'] if 'agent_id' in kwargs and kwargs['agent_id'] else None,
            kwargs['curator_id'] if 'curator_id' in kwargs and kwargs['curator_id'] else None,
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
                    link_telegram = $16,
                    inn = $17
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
            kwargs['link_telegram'] if 'link_telegram' in kwargs else '',
            kwargs['inn'] if 'inn' in kwargs else '',
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
        for tk in { 'company scope', 'company needs', 'personal expertise', 'personal needs', 'licenses', 'hobbies' }:
            ktk = 'tags_1_' + tk.replace(' ', '_')
            calls = []
            calls_data = {}
            if ktk in kwargs and kwargs[ktk] and kwargs[ktk].strip():
                call_data = ' + '.join([ t for t in re.split(r'\s*\+\s*', kwargs[ktk].strip()) if t ])
                calls.append(
                    api.pg.club.execute(
                        """INSERT INTO
                                users_tags_1 (user_id, category, tags)
                            VALUES
                                ($1, $2, $3)
                            ON CONFLICT
                                (user_id, category)
                            DO UPDATE SET
                                tags = EXCLUDED.tags""",
                        id, tk, call_data
                    )
                )
            if calls:
                await asyncio.gather(*calls)
        await self.set(id = id, active = kwargs['active'] if 'active' in kwargs else True)


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
        await api.pg.club.execute(
            """UPDATE users SET active = FALSE WHERE id = $1""",
            self.id
        )


    ################################################################
    async def update_telegram(self, telegram_id):
        api = get_api_context()
        await api.pg.club.execute(
            """UPDATE users_info SET id_telegram = $2 WHERE user_id = $1""",
            self.id, telegram_id
        )


    ################################################################
    async def membership_stage_update(self, stage_id, field, value, author_id = None):
        api = get_api_context()
        if field == 'rejection':
            v = False
            if value == 'true' and stage_id != 0:
                v = True
                await api.pg.club.execute(
                    """UPDATE
                            users_memberships
                        SET
                            rejection = FALSE,
                            postopen = FALSE
                        WHERE
                            user_id = $1""",
                    self.id
                )
            await api.pg.club.execute(
                """INSERT INTO
                        users_memberships (user_id, stage_id, rejection)
                    VALUES
                        ($1, $2, $3)
                    ON CONFLICT
                        (user_id, stage_id)
                    DO UPDATE SET
                        rejection = EXCLUDED.rejection""",
                self.id, stage_id, v
            )
        if field == 'postopen':
            v = False
            if value == 'true' and stage_id != 0:
                v = True
                await api.pg.club.execute(
                    """UPDATE
                            users_memberships
                        SET
                            rejection = FALSE,
                            postopen = FALSE
                        WHERE
                            user_id = $1""",
                    self.id
                )
            await api.pg.club.execute(
                """INSERT INTO
                        users_memberships (user_id, stage_id, postopen)
                    VALUES
                        ($1, $2, $3)
                    ON CONFLICT
                        (user_id, stage_id)
                    DO UPDATE SET
                        postopen = EXCLUDED.postopen""",
                self.id, stage_id, v
            )
        if field == 'repeat':
            v = False
            if value == 'true' and stage_id == 0:
                v = True
            await api.pg.club.execute(
                """INSERT INTO
                        users_memberships (user_id, stage_id, repeat)
                    VALUES
                        ($1, $2, $3)
                    ON CONFLICT
                        (user_id, stage_id)
                    DO UPDATE SET
                        repeat = EXCLUDED.repeat""",
                self.id, stage_id, v
            )
        if field == 'active':
            v = False
            if value == 'true':
                v = True
            await api.pg.club.execute(
                """UPDATE
                        users_memberships
                    SET
                        active = FALSE,
                        rejection = FALSE,
                        postopen = FALSE,
                        repeat = FALSE
                    WHERE
                        user_id = $1""",
                self.id
            )
            await api.pg.club.execute(
                """INSERT INTO
                        users_memberships (user_id, stage_id, active)
                    VALUES
                        ($1, $2, $3)
                    ON CONFLICT
                        (user_id, stage_id)
                    DO UPDATE SET
                        active = EXCLUDED.active
                    """,
                self.id, stage_id, v
            )
            if v and (stage_id == 4 or stage_id == 5):
                tc = await api.pg.club.fetchval(
                    """SELECT
                            coalesce(t2.time_control, t3.time_control, now() at time zone 'utc' + INTERVAL '2 MONTH')
                        FROM
                            users t1
                        LEFT JOIN
                            users_memberships t2 ON t2.user_id = t1.id AND t2.stage_id = 4
                        LEFT JOIN
                            users_memberships t3 ON t3.user_id = t1.id AND t3.stage_id = 5
                        WHERE
                            t1.id = $1""",
                    self.id
                )
                if tc:
                    await api.pg.club.execute(
                        """INSERT INTO
                                users_memberships (user_id, stage_id, time_control)
                            VALUES
                                ($1, $2, $3)
                            ON CONFLICT
                                (user_id, stage_id)
                            DO UPDATE SET
                                time_control = EXCLUDED.time_control""",
                        self.id, 4, tc
                    )
                    await api.pg.club.execute(
                        """INSERT INTO
                                users_memberships (user_id, stage_id, time_control)
                            VALUES
                                ($1, $2, $3)
                            ON CONFLICT
                                (user_id, stage_id)
                            DO UPDATE SET
                                time_control = EXCLUDED.time_control""",
                        self.id, 5, tc
                    )
        if field == 'comment':
            await api.pg.club.execute(
                """INSERT INTO
                        users_memberships (user_id, stage_id, comment, comment_author_id, comment_time)
                    VALUES
                        ($1, $2, $3, $4, now() at time zone 'utc')
                    ON CONFLICT
                        (user_id, stage_id)
                    DO UPDATE SET
                        comment = EXCLUDED.comment,
                        comment_author_id = EXCLUDED.comment_author_id,
                        comment_time = EXCLUDED.comment_time""",
                self.id, stage_id, value, author_id
            )
        if field == 'time_control':
            await api.pg.club.execute(
                """INSERT INTO
                        users_memberships (user_id, stage_id, time_control)
                    VALUES
                        ($1, $2, $3)
                    ON CONFLICT
                        (user_id, stage_id)
                    DO UPDATE SET
                        time_control = EXCLUDED.time_control""",
                self.id, stage_id, int(value) if value else None
            )



    ################################################################
    async def control_update(self, field, value):
        api = get_api_context()
        if field == 'time_control':
            await api.pg.club.execute(
                """INSERT INTO
                        users_managers_reviews (user_id, time_control)
                    VALUES
                        ($1, $2)
                    ON CONFLICT
                        (user_id)
                    DO UPDATE SET
                        time_control = EXCLUDED.time_control""",
                self.id, int(value) if value else None
            )



    ################################################################
    async def membership_rating_update(self, field, value, author_id = None):
        api = get_api_context()
        if field == 'comment':
            await api.pg.club.execute(
                """INSERT INTO
                        users_managers_reviews (user_id, review, review_author_id, review_time)
                    VALUES
                        ($1, $2, $3, now() at time zone 'utc')
                    ON CONFLICT
                        (user_id)
                    DO UPDATE SET
                        review = EXCLUDED.review,
                        review_author_id = EXCLUDED.review_author_id,
                        review_time = EXCLUDED.review_time""",
                self.id, value, author_id
            )
        if field == 'rating':
            await api.pg.club.execute(
                """INSERT INTO
                        users_managers_reviews (user_id, rating_id)
                    VALUES
                        ($1, $2)
                    ON CONFLICT
                        (user_id)
                    DO UPDATE SET
                        rating_id = EXCLUDED.rating_id""",
                self.id, int(value)
            )


    ################################################################
    async def get_membership_stage(self):
        api = get_api_context()
        stage = await api.pg.club.fetchval(
            """SELECT
                    stage_id
                FROM
                    users_memberships
                WHERE
                    user_id = $1 AND
                    active IS TRUE""",
            self.id
        )
        return stage


    ################################################################
    def get_agent_tree(self):
        temp = [ self.id ]
        if self.curator_id:
            temp.extend([ item['id'] for item in self.curators ])
        return temp
    

    ################################################################
    async def get_agent_subs_tree(self):
        api = get_api_context()
        data = await api.pg.club.fetch(
            """SELECT id FROM curators WHERE curators @> ('[{"id": ' || $1 || '}]')::jsonb""",
            str(self.id)
        )
        return [ item['id'] for item in data ]


    ################################################################
    async def get_allowed_clients_ids(self):
        api = get_api_context()
        if not self.check_roles({ 'admin', 'moderator', 'manager', 'chief', 'community manager', 'agent', 'curator' }):
            return []
        clients_ids = None
        if not self.check_roles({ 'admin', 'moderator', 'manager', 'chief' }):
            clients_ids = []
            query = []
            args = []
            i = 1
            if self.check_roles({ 'community manager' }):
                query.append('t1.community_manager_id = $' + str(i))
                args.append(self.id)
                i += 1
            if self.check_roles({ 'agent' }):
                query.append('t1.agent_id = $' + str(i))
                args.append(self.id)
                i += 1
            if self.check_roles({ 'curator' }):
                agents_ids = await self.get_agent_subs_tree()
                if agents_ids:
                    query.append('t1.agent_id = ANY($' + str(i) + ')')
                    args.append(agents_ids)
                    i += 1
            if query:
                query_text = ' OR '.join(query)
                if query_text:
                    query_text = ' AND (' + query_text + ')'
                data = await api.pg.club.fetchval(
                    """SELECT
                            array_agg(t1.id)
                        FROM
                            users t1
                        INNER JOIN
                            users_roles t2 ON t2.user_id = t1.id
                        INNER JOIN
                            roles t3 ON t3.id = t2.role_id
                        WHERE
                            t3.alias = 'client'""" + query_text,
                    *args
                )
                if data:
                    clients_ids.extend(data)
        return clients_ids


    ################################################################
    async def update_event_tags(self, event_id, tags, interests):
        api = get_api_context()
        tags_new = ','.join(list(set([ t for t in re.split(r'\s*,\s*', tags.strip()) if t ])))
        interests_new = ','.join(list(set([ t for t in re.split(r'\s*,\s*', interests.strip()) if t ])))
        await api.pg.club.execute(
            """INSERT INTO
                    users_events_tags (user_id, event_id, tags, interests)
                VALUES
                    ($1, $2, $3, $4)
                ON CONFLICT
                    (user_id, event_id)
                DO UPDATE SET
                    tags = EXCLUDED.tags,
                    interests = EXCLUDED.interests""",
            self.id, event_id, tags_new, interests_new
        )


    ################################################################
    async def view_profile(self, user_id):
        api = get_api_context()
        if self.id != user_id:
            tv = await api.pg.club.fetchval(
                """INSERT INTO
                        users_profiles_views (user_id, target_id)
                    VALUES
                        ($1, $2)
                    RETURNING
                        time_view""",
                self.id, user_id
            )
            if tv:
                return True
        return False


    ################################################################
    async def get_profile_views_amount(self):
        api = get_api_context()
        amount = await api.pg.club.fetchval(
            """SELECT count(*) FROM users_profiles_views WHERE target_id = $1""",
            self.id
        )
        return amount



    ################################################################
    async def get_date_profile_views_amount(self, dt):
        api = get_api_context()
        amount = await api.pg.club.fetchval(
            """SELECT count(*) FROM users_profiles_views WHERE target_id = $1 AND time_view > date ($2) AND time_view < date ($2) + interval '24 hours'""",
            self.id, datetime.strptime(dt, '%Y-%m-%d').date()
        )
        return amount


    ################################################################
    async def get_notifications_1_new(self, mode):
        data = await get_stats(user_id = self.id, mode = mode)
        return data['new']


    ################################################################
    async def get_profile_views_amount(self, dt):
        api = get_api_context()
        data1 = await api.pg.club.fetchrow(
            """SELECT
                    count(*) AS views_all,
                    count(*) FILTER (WHERE time_view > date($2) AND time_view < date($2) + interval '24 hours') AS views_yersterday,
                    count(*) FILTER (WHERE time_view > date($2) - interval '144 hours' AND time_view < date($2) + interval '24 hours') AS views_week,
                    count(*) FILTER (WHERE time_view > date($2) - interval '696 hours' AND time_view < date($2) + interval '24 hours') AS views_month,
                    count(DISTINCT user_id) AS views_all_unique,
                    count(DISTINCT user_id) FILTER (WHERE time_view > date($2) AND time_view < date($2) + interval '24 hours') AS views_yersterday_unique,
                    count(DISTINCT user_id) FILTER (WHERE time_view > date($2) - interval '144 hours' AND time_view < date($2) + interval '24 hours') AS views_week_unique,
                    count(DISTINCT user_id) FILTER (WHERE time_view > date($2) - interval '696 hours' AND time_view < date($2) + interval '24 hours') AS views_month_unique
                FROM
                    users_profiles_views
                WHERE
                    target_id = $1 AND user_id IN (
                        SELECT user_id FROM users_roles WHERE role_id = 10002
                    )""",
            self.id, datetime.strptime(dt, '%Y-%m-%d').date()
        )
        data2 = await api.pg.club.fetchrow(
            """SELECT
                    count(*) AS views_all,
                    count(*) FILTER (WHERE time_view > date($2) AND time_view < date($2) + interval '24 hours') AS views_yersterday,
                    count(*) FILTER (WHERE time_view > date($2) - interval '144 hours' AND time_view < date($2) + interval '24 hours') AS views_week,
                    count(*) FILTER (WHERE time_view > date($2) - interval '696 hours' AND time_view < date($2) + interval '24 hours') AS views_month,
                    count(DISTINCT target_id) AS views_all_unique,
                    count(DISTINCT target_id) FILTER (WHERE time_view > date($2) AND time_view < date($2) + interval '24 hours') AS views_yersterday_unique,
                    count(DISTINCT target_id) FILTER (WHERE time_view > date($2) - interval '144 hours' AND time_view < date($2) + interval '24 hours') AS views_week_unique,
                    count(DISTINCT target_id) FILTER (WHERE time_view > date($2) - interval '696 hours' AND time_view < date($2) + interval '24 hours') AS views_month_unique
                FROM
                    users_profiles_views
                WHERE
                    user_id = $1 AND target_id IN (
                        SELECT user_id FROM users_roles WHERE role_id = 10002
                    )""",
            self.id, datetime.strptime(dt, '%Y-%m-%d').date()
        )
        return {
            'in': {
                'all': data1['views_all'],
                'yersterday': data1['views_yersterday'],
                'week': data1['views_week'],
                'month': data1['views_month'],
                'all_unique': data1['views_all_unique'],
                'yersterday_unique': data1['views_yersterday_unique'],
                'week_unique': data1['views_week_unique'],
                'month_unique': data1['views_month_unique'],
            },
            'out': {
                'all': data2['views_all'],
                'yersterday': data2['views_yersterday'],
                'week': data2['views_week'],
                'month': data2['views_month'],
                'all_unique': data2['views_all_unique'],
                'yersterday_unique': data2['views_yersterday_unique'],
                'week_unique': data2['views_week_unique'],
                'month_unique': data2['views_month_unique'],
            },
        }


    ################################################################
    async def get_profile_views(self):
        api = get_api_context()
        data1 = await api.pg.club.fetch(
            """SELECT
                    t1.time_view, t2.id, t2.name, t3.company, t2.active, t4.hash AS avatar_hash
                FROM
                    users_profiles_views t1
                INNER JOIN
                    users t2 ON t2.id = t1.user_id
                INNER JOIN
                    users_info t3 ON t3.user_id = t1.user_id
                LEFT JOIN
                    avatars t4 ON t4.owner_id = t1.user_id AND t4.active IS TRUE
                WHERE
                    t1.target_id = $1 AND t1.user_id IN (
                        SELECT user_id FROM users_roles WHERE role_id = 10002
                    )
                ORDER BY
                    t1.time_view DESC LIMIT 100""",
            self.id
        )
        data2 = await api.pg.club.fetch(
            """SELECT
                    t1.time_view, t2.id, t2.name, t3.company, t2.active, t4.hash AS avatar_hash
                FROM
                    users_profiles_views t1
                INNER JOIN
                    users t2 ON t2.id = t1.target_id
                INNER JOIN
                    users_info t3 ON t3.user_id = t1.target_id
                LEFT JOIN
                    avatars t4 ON t4.owner_id = t1.target_id AND t4.active IS TRUE
                WHERE
                    t1.user_id = $1 AND t1.target_id IN (
                        SELECT user_id FROM users_roles WHERE role_id = 10002
                    )
                ORDER BY
                    t1.time_view DESC LIMIT 100""",
            self.id
        )
        return {
            'in': [ dict(item) for item in data1 ] if data1 else [],
            'out': [ dict(item) for item in data2 ] if data2 else [],
        }


    ################################################################
    async def get_event_connections_ids(self, event_id):
        api = get_api_context()
        data = await api.pg.club.fetch(
            """SELECT
                    user_1_id, user_2_id
                FROM
                    users_connections
                WHERE
                    (user_1_id = $1 OR user_2_id = $1) AND
                    event_id = $2 AND
                    deleted IS FALSE""",
            self.id, event_id
        )
        temp = []
        if data:
            for item in data:
                if item['user_1_id'] != self.id:
                    temp.append(item['user_1_id'])
                else:
                    temp.append(item['user_2_id'])
        return temp


    ################################################################
    async def get_offline_connections(self):
        api = get_api_context()
        data = await api.pg.club.fetch(
            """SELECT
                    t1.id, t1.event_id, t1.user_1_id, t1.user_2_id, t1.state, t1.rating_1, t1.rating_2,
                    t1.response,
                    t2.name AS user_1_name, t3.name AS user_2_name,
                    t2.active AS user_1_active, t3.active AS user_2_active,
                    t1.creator_id, t4.name AS creator, t1.deleted,
                    t22.id AS community_manager_1_id, t33.id AS community_manager_2_id, 
                    coalesce(t22.name, '') AS community_manager_1,
                    coalesce(t33.name, '') AS community_manager_2,
                    t7.hash AS user_1_avatar_hash,
                    t8.hash AS user_2_avatar_hash,
                    t222.company AS user_1_company,
                    t333.company AS user_2_company,
                    ct.comments
                FROM
                    users_connections t1
                LEFT JOIN
                    users t2 ON t2.id = t1.user_1_id
                LEFT JOIN
                    users_info t222 ON t222.user_id = t1.user_1_id
                LEFT JOIN
                    users t22 ON t22.id = t2.community_manager_id
                LEFT JOIN
                    users t3 ON t3.id = t1.user_2_id
                LEFT JOIN
                    users_info t333 ON t333.user_id = t1.user_2_id
                LEFT JOIN
                    users t33 ON t33.id = t3.community_manager_id
                LEFT JOIN
                    users t4 ON t4.id = t1.creator_id
                LEFT JOIN
                    avatars t7 ON t7.owner_id = t1.user_1_id AND t7.active IS TRUE
                LEFT JOIN
                    avatars t8 ON t8.owner_id = t1.user_2_id AND t8.active IS TRUE
                LEFT JOIN
                    (
                        SELECT
                            ct7.connection_id, array_agg(comment) AS comments
                        FROM (
                            SELECT
                                ct5.connection_id, jsonb_build_object(
                                    'id', ct5.id,
                                    'time_create', ct5.time_create,
                                    'comment', ct5.comment,
                                    'author_id', ct5.author_id,
                                    'author_name', ct6.name
                                ) AS comment
                            FROM
                                connections_comments ct5
                            INNER JOIN
                                users ct6 ON ct6.id = ct5.author_id
                            ORDER BY ct5.id DESC
                        ) ct7
                        GROUP BY ct7.connection_id
                    ) ct ON ct.connection_id = t1.id
                WHERE
                    t1.event_id IS NULL AND
                    t1.deleted IS FALSE AND
                    (t1.user_1_id = $1 OR t1.user_2_id = $1)""",
            self.id
        )
        if data:
            return [ dict(item) for item in data ]
        return []


    ################################################################
    async def get_favorites_to_select(self):
        api = get_api_context()
        exceptions = [ self.id, 10004, 10069, 10077, 10094, 10169, 10170, 10173, 10183, 10295, 10296, 10297, 10298 ]
        data = await api.pg.club.fetch(
            """SELECT
                    t1.id,
                    t1.name,
                    t2.company,
                    t2.catalog,
                    t3.hash AS avatar_hash
                FROM
                    users t1
                INNER JOIN
                    users_info t2 ON t2.user_id = t1.id
                INNER JOIN
                    users_roles t4 ON t4.user_id = t1.id
                INNER JOIN
                    roles t5 ON t5.id = t4.role_id
                LEFT JOIN
                    avatars t3 ON t3.owner_id = t1.id AND t3.active IS TRUE
                WHERE
                    t5.alias = 'client' AND
                    t1.active IS TRUE AND
                    NOT t1.id = ANY($2) AND
                    t1.id NOT IN (
                        SELECT target_id FROM users_favorites WHERE user_id = $1
                    )
                ORDER BY
                    t1.time_create
                LIMIT
                    10""",
            self.id, exceptions
        )
        if data:
            return [ dict(item) for item in data ]
        return []


    ################################################################
    async def set_favorites(self, target_id, flag):
        api = get_api_context()
        if flag is None:
            await api.pg.club.execute(
                """DELETE FROM
                        users_favorites
                    WHERE
                        user_id = $1 AND target_id = $2""",
                self.id, target_id
            )
        else:
            await api.pg.club.execute(
                """INSERT INTO
                        users_favorites (user_id, target_id, flag)
                    VALUES
                        ($1, $2, $3)
                    ON CONFLICT
                        (user_id, target_id)
                    DO UPDATE SET
                        flag = EXCLUDED.flag""",
                self.id, target_id, flag
            )


    ################################################################
    async def get_favorites(self, users_ids):
        api = get_api_context()
        data = await api.pg.club.fetch(
            """SELECT target_id, flag FROM users_favorites WHERE user_id = $1 AND target_id = ANY($2)""",
            self.id, users_ids
        )
        result = {}
        for item in data:
            result[str(item['target_id'])] = item['flag']
        for id in users_ids:
            if str(id) not in result:
                result[str(id)] = None
        return result



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
        query = """t1.id = ANY($1)"""
        args.append(users_ids)
    else:
        query = """('client' = ANY(t4.roles) OR t1.id = 10004) AND t1.active IS TRUE"""
    data = await api.pg.club.fetch(
        """SELECT
                t1.id, t1.time_create, t1.time_update,
                t1.name, t1.login, t1.email, t1.phone,
                t1.active,
                t3.company, t3.position, t3.inn, t3.detail,
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
                t1.password AS _password,
                coalesce(ut1.tags, '') AS tags_1_company_scope,
                coalesce(ut2.tags, '') AS tags_1_company_needs,
                coalesce(ut3.tags, '') AS tags_1_personal_expertise,
                coalesce(ut4.tags, '') AS tags_1_personal_needs,
                coalesce(ut5.tags, '') AS tags_1_licenses,
                coalesce(ut6.tags, '') AS tags_1_hobbies
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
            LEFT JOIN
                users_tags_1 ut1 ON ut1.user_id = t1.id AND ut1.category = 'company scope'
            LEFT JOIN
                users_tags_1 ut2 ON ut2.user_id = t1.id AND ut2.category = 'company needs'
            LEFT JOIN
                users_tags_1 ut3 ON ut3.user_id = t1.id AND ut3.category = 'personal expertise'
            LEFT JOIN
                users_tags_1 ut4 ON ut4.user_id = t1.id AND ut4.category = 'personal needs'
            LEFT JOIN
                users_tags_1 ut5 ON ut5.user_id = t1.id AND ut5.category = 'licenses'
            LEFT JOIN
                users_tags_1 ut6 ON ut6.user_id = t1.id AND ut6.category = 'hobbies'
            WHERE
                """ + query + """
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
async def get_speakers(users_ids = None):
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
                t3.company, t3.position, t3.inn, t3.detail,
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
                t1.password AS _password,
                coalesce(ut1.tags, '') AS tags_1_company_scope,
                coalesce(ut2.tags, '') AS tags_1_company_needs,
                coalesce(ut3.tags, '') AS tags_1_personal_expertise,
                coalesce(ut4.tags, '') AS tags_1_personal_needs,
                coalesce(ut5.tags, '') AS tags_1_licenses,
                coalesce(ut6.tags, '') AS tags_1_hobbies
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
            LEFT JOIN
                users_tags_1 ut1 ON ut1.user_id = t1.id AND ut1.category = 'company scope'
            LEFT JOIN
                users_tags_1 ut2 ON ut2.user_id = t1.id AND ut2.category = 'company needs'
            LEFT JOIN
                users_tags_1 ut3 ON ut3.user_id = t1.id AND ut3.category = 'personal expertise'
            LEFT JOIN
                users_tags_1 ut4 ON ut4.user_id = t1.id AND ut4.category = 'personal needs'
            LEFT JOIN
                users_tags_1 ut5 ON ut5.user_id = t1.id AND ut5.category = 'licenses'
            LEFT JOIN
                users_tags_1 ut6 ON ut6.user_id = t1.id AND ut6.category = 'hobbies'
            WHERE
                ('speaker' = ANY(t4.roles) OR t1.id = 10004)""" + query + """
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
async def get_community_managers_for_report(agent_id = None, community_manager_id = None):
    api = get_api_context()
    where = [ "t3.alias = 'community manager'" ]
    args = []
    i = 1
    if agent_id:
        where.append('t1.agent_id = $' + str(i))
        args.append(agent_id)
        i += 1
    if community_manager_id:
        where.append('t1.id = $' + str(i))
        args.append(community_manager_id)
        i += 1
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
                """ + ' AND '.join(where) + """
            ORDER BY
                t1.name""",
        *args
    )
    return [
        { 'id': item['id'], 'name': item['name'] } for item in data
    ]



################################################################
async def get_agents():
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
                t3.alias = 'agent'
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
    dt_now = datetime.now(tz = pytz.utc)
    if dt_now.month == 1:
        dt_control = datetime(dt_now.year - 1, 11, 1, tzinfo = pytz.utc)
    elif dt_now.month == 2:
        dt_control = datetime(dt_now.year - 1, 12, 1, tzinfo = pytz.utc)
    else:
        dt_control = datetime(dt_now.year, dt_now.month - 2, 1, tzinfo = pytz.utc)
    data = await api.pg.club.fetch(
        """SELECT
                t1.id,
                t4.votes AS votes,
                COALESCE(t5.events_count, 0) AS events_count,
                t2.review, t3.name AS review_author, round(extract(epoch FROM t2.review_time) * 1000)::bigint AS review_time, t2.rating_id,
                round(extract(epoch FROM t2.time_control) * 1000)::bigint AS time_control,
                t6.stages
            FROM
                users t1
            LEFT JOIN
                (
                    SELECT
                        s.user_id, jsonb_agg(s.stage) AS stages
                    FROM
                        (
                            SELECT
                                s1.id AS stage_id, s2.user_id, jsonb_build_object(
                                    'id', s1.id,
                                    'time', round(extract(epoch FROM s2.time_control) * 1000)::bigint,
                                    'rejection', coalesce(s2.rejection, FALSE),
                                    'postopen', coalesce(s2.postopen, FALSE),
                                    'repeat', coalesce(s2.repeat, FALSE),
                                    'active', coalesce(s2.active, FALSE),
                                    'data', jsonb_build_object(
                                        'comment',
                                        CASE
                                            WHEN s2.comment IS NULL THEN
                                                NULL
                                            ELSE
                                                jsonb_build_object(
                                                    'text', s2.comment,
                                                    'author', s3.name,
                                                    'time', round(extract(epoch FROM s2.comment_time) * 1000)::bigint
                                                )
                                        END
                                    )
                                ) AS stage
                            FROM
                                membership_stages s1
                            LEFT JOIN
                                users_memberships s2 ON s2.stage_id = s1.id
                            LEFT JOIN
                                users s3 ON s3.id = s2.comment_author_id
                            ORDER BY s1.id
                        ) s
                    GROUP BY
                        s.user_id
                ) t6 ON t6.user_id = t1.id
            LEFT JOIN
                (
                    SELECT
                        v1.user_id,
                        jsonb_agg(
                            jsonb_build_object(
                                'poll_id', v2.id, 'text', v2.text, 'time_vote', round(extract(epoch FROM v1.time_create) * 1000)::bigint, 'answer', v2.answers[v1.answer]
                            )
                        ) AS votes
                    FROM
                        polls_votes v1
                    INNER JOIN
                        polls v2 ON v2.id = v1.poll_id
                    WHERE
                        v2.active IS TRUE AND
                        v2.rating IS TRUE AND
                        v1.time_create >= $2
                    GROUP BY
                        v1.user_id
                ) t4 ON t4.user_id = t1.id
            LEFT JOIN
                (
                    SELECT
                        t1.user_id, count(t1.event_id) AS events_count
                    FROM
                        events_users t1
                    INNER JOIN
                        events t2 ON t2.id = t1.event_id
                    WHERE
                        t1.audit = 2 AND
                        t2.active IS TRUE AND
                        t2.time_event >= $2
                    GROUP BY t1.user_id
                ) t5 ON t5.user_id = t1.id
            LEFT JOIN
                users_managers_reviews t2 ON t2.user_id = t1.id
            LEFT JOIN
                users t3 ON t3.id = t2.review_author_id
            WHERE
                t1.id = ANY($1)""",
        users_ids, dt_control.timestamp() * 1000
    )
    memberships = {}
    for item in data:
        k = str(item['id'])
        # rating polls
        votes_rating = 3
        votes_answer = None
        if item['votes']:
            temp = None
            for vote in item['votes']:
                if temp is None or temp < vote['time_vote']:
                    temp = vote['time_vote']
                    votes_answer = vote['answer']
        if votes_answer:
            if votes_answer.startswith('{g}'):
                votes_rating = 1
            elif votes_answer.startswith('{y}'):
                votes_rating = 2
        memberships[k] = {
            'rating': 1,
            'semaphore': [
                {
                    'id': 1,
                    'name': 'Оценка менеджера',
                    'rating': item['rating_id'] if item['rating_id'] else None,
                    'data': {
                        'time_control': item['time_control'],
                        'comment': {
                            'text': item['review'],
                            'author': item['review_author'],
                            'time': item['review_time'],
                        } if item['review'] else None,
                    }
                },
                {
                    'id': 2,
                    'name': 'Участие в опросах',
                    'rating': votes_rating,
                    'data': {
                        'value': item['votes'] if item['votes'] else None,
                    }
                },
                {
                    'id': 3,
                    'name': 'Участие в мероприятиях',
                    'rating': 1 if item['events_count'] >= 4 else 2 if item['events_count'] >= 2 else 3,
                    'data': {
                        'value': item['events_count'],
                    }
                },
            ],
            'stage': 1,
            'stages': [
                {
                    'id': i,
                    'time': None,
                    'data': {
                        'comment': None,
                    },
                    'rejection': False,
                    'postopen': False,
                    'repeat': False,
                    'active': False,
                } for i in range(7)
            ],
        }
        matrix = {
            '111': 1,
            '112': 1,
            '113': 2,
            '121': 3,
            '122': 2,
            '123': 3,
            '131': 3,
            '132': 3,
            '133': 3,
            '211': 1,
            '212': 2,
            '213': 3,
            '221': 3,
            '222': 2,
            '223': 3,
            '231': 3,
            '232': 3,
            '233': 3,
            '311': 2,
            '312': 3,
            '313': 3,
            '321': 3,
            '322': 3,
            '323': 3,
            '331': 3,
            '332': 3,
            '333': 3,
        }
        semaphore = memberships[k]['semaphore']
        if semaphore[0]['rating']:
            memberships[k]['rating'] = matrix[str(semaphore[0]['rating']) + str(semaphore[1]['rating']) + str(semaphore[2]['rating'])]
        else:
            memberships[k]['rating'] = matrix['3' + str(semaphore[1]['rating']) + str(semaphore[2]['rating'])]
        if item['stages']:
            for stage in item['stages']:
                for sk in { 'time', 'data', 'rejection', 'postopen', 'repeat', 'active' }:
                    memberships[k]['stages'][stage['id']][sk] = stage[sk]
                if stage['active']:
                    memberships[k]['stage'] = stage['id']
    return memberships



################################################################
async def get_agents_list(community_manager_id, active_only = True):
    api = get_api_context()
    conditions = [ 't1.id >= 10000' ]
    condition_query = ''
    args = []
    if active_only:
        conditions.append('t1.active IS TRUE')
    if community_manager_id:
        conditions.append("""t1.community_manager_id = $""" + str(len(args) + 1))
        args.append(community_manager_id)
    if conditions:
        conditions_query = ' WHERE ' + ' AND '.join(conditions)
    data = await api.pg.club.fetch(
        """SELECT
                s1.*, s2.hash AS avatar
            FROM (
                SELECT
                    t1.id, t1.name,
                    jsonb_agg(json_build_object('id', t2.id, 'name', t2.name, 'company', t3.company, 'community_manager_id', coalesce(t4.id, 0))) AS clients
                FROM
                    users t1
                INNER JOIN
                    users t2 ON t2.agent_id = t1.id
                INNER JOIN
                    users_info t3 ON t3.user_id = t2.id
                LEFT JOIN
                    users t4 ON t4.id = t2.community_manager_id
                """ + conditions_query + """
                GROUP BY
                    t1.id
            ) s1
            LEFT JOIN
                avatars s2 ON s2.owner_id = s1.id AND s2.active IS TRUE
            ORDER BY
                    s1.name""",
        *args
    )
    return [
        {
            'id': item['id'],
            'name': item['name'],
            'clients': sorted(item['clients'], key=lambda x: x['name']),
            'avatar_hash': item['avatar'],
        }
        for item in data
    ]



################################################################
async def create_connection(event_id, user_1_id, user_2_id, creator_id):
    api = get_api_context()
    if user_1_id != user_2_id:
        id = await api.pg.club.fetchval(
            """INSERT INTO 
                    users_connections (event_id, user_1_id, user_2_id, creator_id)
                VALUES
                    ($1, $2, $3, $4)
                ON CONFLICT
                    (event_id, user_1_id, user_2_id)
                DO UPDATE SET
                    deleted = FALSE
                RETURNING
                    id""",
            event_id,
            user_1_id if user_1_id < user_2_id else user_2_id,
            user_2_id if user_1_id < user_2_id else user_1_id,
            creator_id,
        )
        return id
    return None



################################################################
async def create_offline_connection(user_1_id, user_2_id, creator_id):
    api = get_api_context()
    if user_1_id != user_2_id:
        check = await api.pg.club.fetchrow(
            """SELECT
                    id, creator_id, deleted
                FROM
                    users_connections
                WHERE
                    (
                        (user_1_id = $1 AND user_2_id = $2) OR
                        (user_1_id = $2 AND user_2_id = $1)
                    ) AND
                    event_id IS NULL""",
            user_1_id, user_2_id
        )
        if check:
            query = []
            args = []
            if check['creator_id'] != creator_id:
                query.append('creator_id = $1')
                args.append(creator_id)
            if check['deleted']:
                query.append('deleted = FALSE')
            if query:
                args.append(check['id'])
                await api.pg.club.fetchval(
                    """UPDATE
                            users_connections
                        SET
                            """ + ', '.join(query) + """
                        WHERE
                            id = $""" + str(len(args)),
                    *args
                )
        else:
            id = await api.pg.club.fetchval(
                """INSERT INTO 
                        users_connections (event_id, user_1_id, user_2_id, creator_id)
                    VALUES
                        ($1, $2, $3, $4)
                    RETURNING
                        id""",
                None,
                user_1_id if user_1_id < user_2_id else user_2_id,
                user_2_id if user_1_id < user_2_id else user_1_id,
                creator_id,
            )
            return id
    return None



################################################################
async def drop_connection(event_id, user_1_id, user_2_id):
    api = get_api_context()
    query = 'event_id IS NULL'
    args = []
    if event_id:
        query = 'event_id = $3'
        args.append(event_id)
    await api.pg.club.fetchval(
        """UPDATE
                users_connections
            SET
                deleted = TRUE
            WHERE
                user_1_id = $1 AND user_2_id = $2 AND """ + query,
        user_1_id if user_1_id < user_2_id else user_2_id,
        user_2_id if user_1_id < user_2_id else user_1_id,
        *args
    )



################################################################
async def update_connection_response(event_id, user_1_id, user_2_id, resp):
    api = get_api_context()
    query = 'event_id IS NULL'
    args = [ resp ]
    if event_id:
        query = 'event_id = $4'
        args.append(event_id)
    await api.pg.club.fetchval(
        """UPDATE
                users_connections
            SET
                response = $3
            WHERE
                user_1_id = $1 AND user_2_id = $2 AND """ + query,
        user_1_id if user_1_id < user_2_id else user_2_id,
        user_2_id if user_1_id < user_2_id else user_1_id,
        *args
    )



################################################################
async def recover_connection(event_id, user_1_id, user_2_id):
    api = get_api_context()
    await api.pg.club.fetchval(
        """UPDATE
                users_connections
            SET
                deleted = FALSE
            WHERE
                event_id = $1 AND user_1_id = $2 AND user_2_id = $3""",
        event_id,
        user_1_id if user_1_id < user_2_id else user_2_id,
        user_2_id if user_1_id < user_2_id else user_1_id,
    )



################################################################
async def update_connection_state(connection_id, state):
    api = get_api_context()
    await api.pg.club.execute(
        """UPDATE
                users_connections
            SET
                state = $2
            WHERE
                id = $1""",
        connection_id, state
    )



################################################################
async def update_connection_comment(connection_id, comment, author_id):
    api = get_api_context()
    await api.pg.club.execute(
        """INSERT INTO
                connections_comments (connection_id, comment, author_id)
            VALUES
                ($1, $2, $3)""",
        connection_id, comment, author_id
    )



################################################################
async def update_connection_rating(connection_id, part, rating):
    api = get_api_context()
    if part == 1:
        await api.pg.club.execute(
            """UPDATE
                    users_connections
                SET
                    rating_1 = $2
                WHERE
                    id = $1""",
            connection_id, rating
        )
    if part == 2:
        await api.pg.club.execute(
            """UPDATE
                    users_connections
                SET
                    rating_2 = $2
                WHERE
                    id = $1""",
            connection_id, rating
        )
    if part == 3:
        await api.pg.club.execute(
            """UPDATE
                    users_connections
                SET
                    rating_1 = $2,
                    rating_2 = $2
                WHERE
                    id = $1""",
            connection_id, rating
        )



################################################################
async def get_connections(ids = None, events_ids = None, users_ids = None):
    api = get_api_context()
    query_string = ''
    query = []
    args = []
    i = 1
    if ids:
        query.append('t1.id = ANY($' + str(i) + ')')
        args.append(ids)
        i += 1
    if events_ids:
        query.append('t1.event_id = ANY($' + str(i) + ')')
        args.append(events_ids)
        i += 1
    if users_ids:
        query.append('(t1.user_1_id = ANY($' + str(i) + ') OR t1.user_2_id = ANY($' + str(i) + '))')
        args.append(users_ids)
        i += 1
    if query:
        query_string = ' WHERE ' + ' AND '.join(query)
    data = await api.pg.club.fetch(
        """SELECT
                t1.id, t1.event_id, t1.user_1_id, t1.user_2_id, t1.state, t1.rating_1, t1.rating_2,
                t1.response,
                t1.creator_id, t4.name AS creator, t1.deleted,
                t22.id AS community_manager_1_id, t33.id AS community_manager_2_id, 
                coalesce(t22.name, '') AS community_manager_1,
                coalesce(t33.name, '') AS community_manager_2,
                t8.comments
            FROM
                users_connections t1
            LEFT JOIN
                users t2 ON t2.id = t1.user_1_id
            LEFT JOIN
                users t22 ON t22.id = t2.community_manager_id
            LEFT JOIN
                users t3 ON t3.id = t1.user_2_id
            LEFT JOIN
                users t33 ON t33.id = t3.community_manager_id
            LEFT JOIN
                users t4 ON t4.id = t1.creator_id
            LEFT JOIN
                (
                    SELECT
                        t7.connection_id, array_agg(comment) AS comments
                    FROM (
                        SELECT
                            t5.connection_id, jsonb_build_object(
                                'id', t5.id,
                                'time_create', t5.time_create,
                                'comment', t5.comment,
                                'author_id', t5.author_id,
                                'author_name', t6.name
                            ) AS comment
                        FROM
                            connections_comments t5
                        INNER JOIN
                            users t6 ON t6.id = t5.author_id
                        ORDER BY t5.id DESC
                    ) t7
                    GROUP BY t7.connection_id
                ) t8 ON t8.connection_id = t1.id
            """ + query_string,
        *args
    )
    if data:
        return [ dict(item) for item in data ]
    return []



################################################################
async def get_profiles_views_amount(users_ids):
    api = get_api_context()
    data = await api.pg.club.fetch(
        """SELECT
                target_id, count(time_view) AS amount
            FROM
                users_profiles_views
            WHERE
                target_id = ANY($1)
            GROUP BY
                target_id""",
        users_ids
    )
    return { str(item['target_id']): item['amount'] for item in data }



################################################################
async def get_date_profiles_views_amount(users_ids, dt):
    api = get_api_context()
    data = await api.pg.club.fetch(
        """SELECT
                target_id, count(time_view) AS amount
            FROM
                users_profiles_views
            WHERE
                target_id = ANY($1) AND time_view > date ($2) AND time_view < date ($2) + interval '24 hours'
            GROUP BY
                target_id""",
        users_ids, datetime.strptime(dt, '%Y-%m-%d').date()
    )
    return { str(item['target_id']): item['amount'] for item in data }



################################################################
async def get_connections_for_report(events_ids = None, users_ids = None):
    api = get_api_context()
    result = {}
    query_string = ''
    query = [ 't1.deleted IS FALSE' ]
    args = []
    i = 1
    if events_ids:
        query.append('t1.event_id = ANY($' + str(i) + ')')
        args.append(events_ids)
        i += 1
    if users_ids:
        query.append('(t1.user_1_id = ANY($' + str(i) + ') OR t1.user_2_id = ANY($' + str(i) + '))')
        args.append(users_ids)
        i += 1
    if query:
        query_string = ' WHERE ' + ' AND '.join(query)
    data = await api.pg.club.fetch(
        """SELECT
                t1.id, t1.event_id, t1.user_1_id, t1.user_2_id, t1.state, t1.rating_1, t1.rating_2, t1.creator_id, t1.response
            FROM
                users_connections t1
            """ + query_string,
        *args
    )
    if data:
        for item in data:
            if str(item['user_1_id']) not in result:
                result[str(item['user_1_id'])] = {
                    'all': set(),
                    'fulfilled': set(),
                }
            if str(item['user_2_id']) not in result:
                result[str(item['user_2_id'])] = {
                    'all': set(),
                    'fulfilled': set(),
                }
            result[str(item['user_1_id'])]['all'].add(item['user_2_id'])
            result[str(item['user_2_id'])]['all'].add(item['user_1_id'])
            if item['state']:
                result[str(item['user_1_id'])]['fulfilled'].add(item['user_2_id'])
                result[str(item['user_2_id'])]['fulfilled'].add(item['user_1_id'])
    return result



################################################################
async def get_all_clients():
    api = get_api_context()
    data = await api.pg.club.fetch(
        """SELECT
                t1.id, t1.name, t1.active, t2.company, t3.hash AS avatar_hash,
                t4.id AS community_manager_id, t4.name AS community_manager_name
            FROM
                users t1
            INNER JOIN
                users_info t2 ON t2.user_id = t1.id
            INNER JOIN
                users_roles t5 ON t5.user_id = t1.id AND t5.role_id = 10002
            LEFT JOIN
                avatars t3 ON t3.owner_id = t1.id AND t3.active IS TRUE
            LEFT JOIN
                users t4 ON t4.id = t1.community_manager_id
            ORDER BY
                t1.name"""
    )
    if data:
        return [ dict(item) for item in data ]
    return []



################################################################
async def update_suggestion(user_id, partner_id, state):
    api = get_api_context()
    await api.pg.club.execute(
        """INSERT INTO
                users_suggestions (user_id, partner_id, state)
            VALUES
                ($1, $2, $3)
            ON CONFLICT
                (user_id, partner_id)
            DO UPDATE SET
                state = EXCLUDED.state""",
        user_id, partner_id, state
    )



################################################################
async def update_suggestion_comment(user_id, partner_id, comment, author_id):
    api = get_api_context()
    await api.pg.club.execute(
        """INSERT INTO
                users_suggestions (user_id, partner_id, state)
            VALUES
                ($1, $2, NULL)
            ON CONFLICT
                (user_id, partner_id)
            DO NOTHING""",
        user_id, partner_id
    )
    await api.pg.club.execute(
        """INSERT INTO
                suggestions_comments (user_id, partner_id, comment, author_id)
            VALUES
                ($1, $2, $3, $4)""",
        user_id, partner_id, comment, author_id
    )



################################################################
async def get_users_with_avatars(users_ids):
    api = get_api_context()
    query = [ 't2.active IS TRUE' ]
    args = []
    if users_ids is not None:
        query.append('t2.id = ANY($1)')
        args.append(users_ids)
    data = await api.pg.club.fetch(
        """SELECT
                t2.id,
                t2.name,
                t22.company,
                t22.catalog,
                t22.annual,
                t5.hash,
                coalesce(t3.tags, '') AS tags,
                coalesce(t3.interests, '') AS interests,
                t2.community_manager_id,
                coalesce(t4.name, '') AS community_manager,
                t2.active,
                coalesce(tm.stage_id, tm2.stage_id) AS stage,
                coalesce(tm.rejection, tm2.rejection) AS rejection
            FROM
                users t2
            INNER JOIN
                users_roles t2r ON t2r.user_id = t2.id
            INNER JOIN
                roles r ON r.id = t2r.role_id AND r.alias = 'client'
            INNER JOIN
                users_info t22 ON t22.user_id = t2.id
            INNER JOIN
                users_tags t3 ON t3.user_id = t2.id
            LEFT JOIN
                users_memberships tm ON tm.user_id = t2.id AND tm.active IS TRUE
            LEFT JOIN
                users_memberships tm2 ON tm2.user_id = t2.id AND tm2.stage_id = 1
            LEFT JOIN
                users t4 ON t4.id = t2.community_manager_id
            LEFT JOIN
                avatars t5 ON t5.owner_id = t2.id AND t5.active IS TRUE
            WHERE
                """ + ' AND '.join(query),
        *args
    )
    return [ dict(item) for item in data ]


################################################################
async def get_favorites_stats(users_ids):
    api = get_api_context()
    data = await api.pg.club.fetch(
        """SELECT
                target_id, count(user_id) AS amount
            FROM
                users_favorites
            WHERE
                target_id = ANY($1)
            GROUP BY
                target_id""",
        users_ids
    )
    result = { str(item['target_id']): item['amount'] for item in data }
    for id in users_ids:
        if str(id) not in result:
            result[str(id)] = 0
    return result
