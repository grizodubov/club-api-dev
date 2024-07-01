import uuid
import pytz
from datetime import datetime

from pandas import DataFrame

from app.core.context import get_api_context
from app.models.user import get_users_memberships, get_last_activity
from app.models.event import Event, get_participants_for_report


STAGES = [
    'У агента', 'Соискатель', 'Адмиссия', 'Подключение', 'Кандидат', 'Оплата', 'Член клуба'
]


EVENTS = {
    'tender': 'Тендерный разбор',
    'place': 'Встреча с ЭТП',
    'meeting': 'Встреча с заказчиком',
    'club': 'Форум-группа',
    'network': 'Отраслевой нетворкинг',
    'expert': 'Экспертная встреча',
    'breakfast': 'Кофе и бизнес',
    'mission': 'Бизнес-миссия в регионах',
    'education': 'Академия закупок',
    'guest': 'Специальный гость',
    'forum': 'Форум АУЗ',
    'webinar': 'Прямой эфир',
}


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
        'Оборот': 't4.annual',
        'Должность': 't4.position',
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

    event = None
    events_users_ids = None
    events_users_cache = {}

    if { 'Мероприятие', 'Присутствие на мероприятии' } & set(config.keys()):
        if 'Мероприятие' in config and config['Мероприятие']['value'] != '0':
            event = Event()
            await event.set(id = int(config['Мероприятие']['value']))
        events_participants = await get_participants_for_report(None if config['Мероприятие']['value'] == '0' else [ int(config['Мероприятие']['value']) ])
        events_users_ids = []
        for k, v in events_participants.items():
            for p in v:
                if str(p['id']) not in events_users_cache:
                    events_users_cache[str(p['id'])] = {
                        'Пришёл': 0,
                        'Не пришёл': 0,
                    }
                if p['audit'] == 2:
                    events_users_cache[str(p['id'])]['Пришёл'] += 1
                else:
                    events_users_cache[str(p['id'])]['Не пришёл'] += 1
                events_users_ids.append(p['id'])
        if events_users_ids:
            query.append('t1.id = ANY($' + str(i) + ')')
            args.append(list(set(events_users_ids)))

    if columns and (events_users_ids is None or events_users_ids):
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

        if { 'Стадия', 'Контрольная дата' } & set(config.keys()):
            memberships = await get_users_memberships(users_ids)
        
        if { 'Активность' } & set(config.keys()):
            activity = await get_last_activity(users_ids = users_ids)

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
                            if dt1.year * 10000 + dt1.month * 100 + dt1.day > dt2.year * 10000 + dt2.month * 100 + dt2.day:
                                continue
                        elif config['Контрольная дата (модификатор)']['value'] == '3':
                            if dt1.year * 10000 + dt1.month * 100 + dt1.day < dt2.year * 10000 + dt2.month * 100 + dt2.day:
                                continue
                if config['Контрольная дата']['report']:
                    temp.update({ 'date_control': tc })
            
            # Стадия
            if 'Стадия' in config:
                tc = memberships[str(item['id'])]['stage']
                if config['Стадия']['filter']:
                    if config['Стадия']['value'] != '1000':
                        if int(config['Стадия']['value']) != tc:
                            continue
                if config['Стадия']['report']:
                    temp.update({ 'stage': STAGES[tc] })

            # Активность
            if 'Активность' in config:
                tc = activity[str(item['id'])] if str(item['id']) in activity else None
                if config['Активность']['filter']:
                    if config['Активность (модификатор)']['value'] == '4':
                        if tc:
                            continue
                    else:
                        if not config['Активность']['value'] or not tc:
                            continue
                        dt1 = datetime.fromtimestamp(round(config['Активность']['value'] / 1000), pytz.utc)
                        dt2 = datetime.fromtimestamp(round(tc / 1000), pytz.utc)
                        if config['Активность (модификатор)']['value'] == '1':
                            if dt1.year != dt2.year or dt1.month != dt2.month or dt1.day != dt2.day:
                                continue
                        elif config['Активность (модификатор)']['value'] == '2':
                            if dt1.year * 10000 + dt1.month * 100 + dt1.day >= dt2.year * 10000 + dt2.month * 100 + dt2.day:
                                continue
                        elif config['Активность (модификатор)']['value'] == '3':
                            if dt1.year * 10000 + dt1.month * 100 + dt1.day < dt2.year * 10000 + dt2.month * 100 + dt2.day:
                                continue
                if config['Активность']['report']:
                    temp.update({ 'time_last_activity': tc })
                
            # Мероприятие
            if 'Мероприятие' in config:
                if config['Мероприятие']['report']:
                    if config['Мероприятие']['value'] == '0':
                        temp.update({ 'event': 'Все мероприятия' })
                    else:
                        if event and event.id:
                            dt = datetime.fromtimestamp(round(event.time_event / 1000), pytz.utc)
                            info = '{:04d}'.format(dt.year) + '-' + '{:02d}'.format(dt.month) + '-' + '{:02d}'.format(dt.day)
                            info += "\n" + EVENTS[event.format]
                            info += "\n" + event.name
                            info += "\n" + event.place
                            temp.update({ 'event': info })
                        else:
                            temp.update({ 'event': '' })

            # Присутствие на мероприятии
            if 'Присутствие на мероприятии' in config:
                if config['Присутствие на мероприятии']['report']:
                    if config['Присутствие на мероприятии']['value'] == '1000':
                        pass
                        #temp.update({ 'event_participation': 'Все мероприятия' })
                    else:
                        pass
                        

            result.append(temp)

    return result



################################################################
def create_clients_file(data):

    FIELDS = {
        'name': 'Имя',
        'company': 'Компания',
        'inn': 'ИНН',
        'annual': 'Оборот',
        'position': 'Должность',
        'email': 'Email',
        'phone': 'Телефон',
        'link_telegram': 'Telegram ID',
        'stage': 'Стадия',
        'date_control': 'Контрольная дата',
        'event': 'Мероприятие',
        'time_last_activity': 'Активность',
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
                
                # time_last_activity
                if k == 'time_last_activity':
                    if v:
                        dt = datetime.fromtimestamp(round(v / 1000), pytz.utc)
                        v = '{:04d}'.format(dt.year) + '-' + '{:02d}'.format(dt.month) + '-' + '{:02d}'.format(dt.day) + ' ' + str(dt.time())
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
#     name: 'Должность',
#     type: 'text',
#     column: 'position',
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
