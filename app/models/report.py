import uuid
import pytz
from datetime import datetime

from pandas import DataFrame

from app.core.context import get_api_context
from app.models.user import get_users_memberships, get_last_activity



################################################################
async def get_clients(config, clients_ids):
    api = get_api_context()

    columns = [ 't1.id', 't1.name AS avatar_name', 't5.hash AS avatar_hash' ]
    query = []
    args = []
    i = 1

    if clients_ids is not None:
        if clients_ids:
            query.append('t1.id = ANY($' + str(i) + ')')
            args.append(clients_ids)
            i += 1
        else:
            return []

    FIELDS1 = {
        'Фамилия / Имя': 't1.name',
        'Компания': 't4.company',
        'ИНН': 't4.inn',
        'Email': 't1.email',
        'Телефон': 't1.phone',
        'Telegram ID': 't4.link_telegram',
    }
    for k in FIELDS1.keys():
        if k in config:
            if config[k]['report']:
                columns.append(FIELDS1[k])
            if config[k]['filter'] and config[k]['value']:
                query.append(FIELDS1[k] + """ ILIKE concat('%', $""" + str(i) + """::text, '%')""")
                args.append(config[k]['value'])
                i += 1

    result = []

    if columns:
        query.append("""t3.alias = 'client'""")
        data = await api.pg.club.fetch(
            """SELECT
                    """ + ', '.join(columns) + """
                FROM
                    users t1
                INNER JOIN
                    users_roles t2 ON t2.user_id = t1.id
                INNER JOIN
                    roles t3 ON t3.id = t2.role_id
                INNER JOIN
                    users_info t4 ON t4.user_id = t1.id
                LEFT JOIN
                    avatars t5 ON t5.owner_id = t1.id AND t5.active IS TRUE
                WHERE
                    """ + ' AND '.join(query),
            *args
        )
        if data:
            users_ids = [ item['id'] for item in data ]
    
    if users_ids:
        if { 'Контрольная дата' } & set(config.keys()):
            memberships = await get_users_memberships(users_ids)

        for item in data:
            temp = dict(item).copy()

            # Контрольная дата
            if 'Контрольная дата' in config:
                tc = memberships[str(item['id'])]['semaphore'][0]['data']['time_control']
                if config['Контрольная дата']['filter']:
                    if config['Контрольная дата (модификатор)']['value'] == '4':
                        if tc:
                            continue
                    else:
                        if not config['Контрольная дата']['value'] or not tc:
                            continue
                        dt1 = datetime.fromtimestamp(round(config['Контрольная дата']['value'] / 1000), pytz.utc)
                        dt2 = datetime.fromtimestamp(round(tc / 1000), pytz.utc)
                        if config['Контрольная дата (модификатор)']['value'] == '1':
                            if dt1.year != dt2.year or dt1.month != dt2.month or dt1.day != dt2.day:
                                continue
                        elif config['Контрольная дата (модификатор)']['value'] == '2':
                            if dt1.year * 10000 + dt1.month * 100 + dt1.day >= dt2.year * 10000 + dt2.month * 100 + dt2.day:
                                continue
                        elif config['Контрольная дата (модификатор)']['value'] == '3':
                            if dt1.year * 10000 + dt1.month * 100 + dt1.day <= dt2.year * 10000 + dt2.month * 100 + dt2.day:
                                continue
                if config['Контрольная дата']['report']:
                    temp.update({ 'date_control': tc })
            
            result.append(temp)

    return result



################################################################
def create_clients_file(data):

    FIELDS = {
        'name': 'Имя',
        'company': 'Компания',
        'inn': 'ИНН',
        'email': 'Email',
        'phone': 'Телефон',
        'link_telegram': 'Telegram ID',
        'date_control': 'Контрольная дата',
    }

    table = {}

    for item in data:
        for k, v in item.items():
            if k in FIELDS:

                # date_control
                if k == 'date_control':
                    if v:
                        dt = datetime.fromtimestamp(round(v / 1000), pytz.utc)
                        v = '{:04d}'.format(dt.year) + '-' + '{:02d}'.format(dt.month) + '-' + '{:02d}'.format(dt.day)
                    else:
                        v = ''

                if FIELDS[k] in table:
                    table[FIELDS[k]].append(v)
                else:
                    table[FIELDS[k]] = [ v ]

    uid = str(uuid.uuid4())

    df = DataFrame(table)
    df.to_excel('/var/www/media.clubgermes.ru/html/reports/' + uid + '.xlsx', sheet_name = 'sheet1', index = False)

    return 'https://media.clubgermes.ru/reports/' + uid + '.xlsx'



# {
#     name: 'Фамилия / Имя',
#     type: 'text',
#     column: 'name',
# },
# {
#     name: 'Компания',
#     type: 'text',
#     column: 'company',
# },
# {
#     name: 'ИНН',
#     type: 'text',
#     column: 'inn',
# },
# {
#     name: 'Email',
#     type: 'text',
#     column: 'email',
# },
# {
#     name: 'Телефон',
#     type: 'text',
#     column: 'phone',
# },
# {
#     name: 'Telegram ID',
#     type: 'text',
#     column: 'link_telegram',
# },
# {
#     name: 'Контрольная дата',
#     type: 'date',
#     column: 'date_control',
#     modificator: {
#         name: 'Контрольная дата (модификатор)',
#         type: 'select',
#         column: '',
#         options: [ { value: '1', text: 'Совпадение' }, { value: '2', text: 'Больше' }, { value: '3', text: 'Меньше' } ],
#         default: '1',
#     }
# },



################################################################
async def get_event(event_id, clients_ids):
    api = get_api_context()

    data = await api.pg.club.fetch(
        """SELECT
                t1.user_id AS id, t2.name, t2.phone, t2.email,
                t3.company, t3.position, t3.inn, t3.annual,

            FROM
                events_users t1
            INNER JOIN
                users t2 ON t2.id = t1.user_id
            INNER JOIN
                users_info t3 ON t3.user_id = t1.user_id


                """
    )




activity = await get_last_activity(users_ids = users_ids)
            users = []
            for item in result:
                user_activity = { 'time_last_activity': activity[str(item.id)] if str(item.id) in activity else None }



 """SELECT
                t1.event_id,
                array_agg(jsonb_build_object(
                    'id', t1.user_id,
                    'name', t2.name,
                    'company', t22.company,
                    'catalog', t22.catalog,
                    'annual', t22.annual,
                    'confirmation', t1.confirmation,
                    'audit', t1.audit,
                    'guests', t1.guests,
                    'avatar_hash', t5.hash,
                    'tags', coalesce(t3.tags, ''),
                    'interests', coalesce(t3.interests, ''),
                    'tags_event', coalesce(t6.tags, ''),
                    'interests_event', coalesce(t6.interests, ''),
                    'community_manager_id', t2.community_manager_id,
                    'community_manager', coalesce(t4.name, '')
                )) AS participants
            FROM
                events_users t1
            INNER JOIN
                users t2 ON t2.id = t1.user_id
            INNER JOIN
                users_info t22 ON t22.user_id = t2.id
            INNER JOIN
                users_tags t3 ON t3.user_id = t2.id
            LEFT JOIN
                users_events_tags t6 ON t6.user_id = t2.id AND t6.event_id = t1.event_id
            LEFT JOIN
                users t4 ON t4.id = t2.community_manager_id
            LEFT JOIN
                avatars t5 ON t5.owner_id = t2.id AND t5.active IS TRUE
            WHERE
                t1.event_id = ANY($1) AND t2.active IS TRUE
            GROUP BY
                t1.event_id""",
        events_ids
    )
    return { str(item['event_id']): item['participants'] for item in data }




    columns = [ 't1.id', 't1.name AS avatar_name', 't5.hash AS avatar_hash' ]
    query = []
    args = []
    i = 1

    if clients_ids is not None:
        if clients_ids:
            query.append('t1.id = ANY($' + str(i) + ')')
            args.append(clients_ids)
            i += 1
        else:
            return []

    FIELDS1 = {
        'Фамилия / Имя': 't1.name',
        'Компания': 't4.company',
        'ИНН': 't4.inn',
        'Email': 't1.email',
        'Телефон': 't1.phone',
        'Telegram ID': 't4.link_telegram',
    }
    for k in FIELDS1.keys():
        if k in config:
            if config[k]['report']:
                columns.append(FIELDS1[k])
            if config[k]['filter'] and config[k]['value']:
                query.append(FIELDS1[k] + """ ILIKE concat('%', $""" + str(i) + """::text, '%')""")
                args.append(config[k]['value'])
                i += 1

    result = []

    if columns:
        query.append("""t3.alias = 'client'""")
        data = await api.pg.club.fetch(
            """SELECT
                    """ + ', '.join(columns) + """
                FROM
                    users t1
                INNER JOIN
                    users_roles t2 ON t2.user_id = t1.id
                INNER JOIN
                    roles t3 ON t3.id = t2.role_id
                INNER JOIN
                    users_info t4 ON t4.user_id = t1.id
                LEFT JOIN
                    avatars t5 ON t5.owner_id = t1.id AND t5.active IS TRUE
                WHERE
                    """ + ' AND '.join(query),
            *args
        )
        if data:
            users_ids = [ item['id'] for item in data ]
    
    if users_ids:
        if { 'Контрольная дата' } & set(config.keys()):
            memberships = await get_users_memberships(users_ids)

        for item in data:
            temp = dict(item).copy()

            # Контрольная дата
            if 'Контрольная дата' in config:
                tc = memberships[str(item['id'])]['semaphore'][0]['data']['time_control']
                if config['Контрольная дата']['filter']:
                    if config['Контрольная дата (модификатор)']['value'] == '4':
                        if tc:
                            continue
                    else:
                        if not config['Контрольная дата']['value'] or not tc:
                            continue
                        dt1 = datetime.fromtimestamp(round(config['Контрольная дата']['value'] / 1000), pytz.utc)
                        dt2 = datetime.fromtimestamp(round(tc / 1000), pytz.utc)
                        if config['Контрольная дата (модификатор)']['value'] == '1':
                            if dt1.year != dt2.year or dt1.month != dt2.month or dt1.day != dt2.day:
                                continue
                        elif config['Контрольная дата (модификатор)']['value'] == '2':
                            if dt1.year * 10000 + dt1.month * 100 + dt1.day >= dt2.year * 10000 + dt2.month * 100 + dt2.day:
                                continue
                        elif config['Контрольная дата (модификатор)']['value'] == '3':
                            if dt1.year * 10000 + dt1.month * 100 + dt1.day <= dt2.year * 10000 + dt2.month * 100 + dt2.day:
                                continue
                if config['Контрольная дата']['report']:
                    temp.update({ 'date_control': tc })
            
            result.append(temp)

    return result
