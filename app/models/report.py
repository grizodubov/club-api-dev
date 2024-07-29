import uuid
import pytz
from datetime import datetime

from pandas import DataFrame

from app.core.context import get_api_context
from app.models.user import get_users_memberships, get_last_activity, get_connections_for_report
from app.models.event import Event, get_participants_for_report
from app.models.note import get_notes_for_report


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

    columns = [ 't1.id', 't1.active', 't1.name AS avatar_name', 't3.alias AS role', 't5.hash AS avatar_hash', 't6.name AS community_manager' ]
    #query = [ 't1.active IS TRUE' ]
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
        'Отрасль': 't4.catalog',
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

    if 'Роль' not in config or not config['Роль']['filter'] or (config['Роль']['filter'] and config['Роль']['value'] == 'client'):
        query.append("""t3.alias = 'client'""")
    else:
        if config['Роль']['filter'] and config['Роль']['value'] == 'applicant':
            query.append("""t3.alias = 'applicant'""")
        if config['Роль']['filter'] and config['Роль']['value'] == 'guest':
            query.append("""t3.alias = 'guest'""")

    if 'Активен' in config and config['Активен']['filter'] and config['Активен']['value'] != '1000':
        if config['Активен']['value'] == '0':
            query.append('t1.active IS FALSE')
        if config['Активен']['value'] == '1':
            query.append('t1.active IS TRUE')

    if 'Коммьюнити-менеджер' in config and config['Коммьюнити-менеджер']['filter'] and config['Коммьюнити-менеджер']['value'] != '0':
        query.append('t1.community_manager_id = $' + str(i))
        args.append(int(config['Коммьюнити-менеджер']['value']))
        i += 1

    if { 'Мероприятие', 'Присутствие на мероприятии', 'Назначенные встречи', 'Состоявшиеся встречи' } & set(config.keys()):
        events_ids = None
        audit_flag = None
        if 'Мероприятие' in config and config['Мероприятие']['filter'] and config['Мероприятие']['value'] != '0':
            event = Event()
            await event.set(id = int(config['Мероприятие']['value']))
            if event.id:
                events_ids = [ event.id ]
        if 'Присутствие на мероприятии' in config and config['Присутствие на мероприятии']['filter'] and config['Присутствие на мероприятии']['value'] != '1000':
            if config['Присутствие на мероприятии']['value'] == '1':
                audit_flag = True
            else:
                audit_flag = False
        events_participants = await get_participants_for_report(events_ids, audit_flag)
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
    
    users_ids = []

    where = ''
    if query:
        where = 'WHERE ' + ' AND '.join(query)

    if columns and (events_users_ids is None or events_users_ids):
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
                LEFT JOIN
                    users t6 ON t6.id = t1.community_manager_id
                """ + where + """
                ORDER BY
                    t1.name""",
            *args
        )
        if data:
            users_ids = [ item['id'] for item in data ]
    
    if users_ids:

        if { 'Стадия', 'Отказ', 'Контрольная дата', 'Пробный период (дней)' } & set(config.keys()):
            memberships = await get_users_memberships(users_ids)
        
        if { 'Активность' } & set(config.keys()):
            activity = await get_last_activity(users_ids = users_ids)
        
        if { 'Назначенные встречи', 'Состоявшиеся встречи' } & set(config.keys()):
            connections = await get_connections_for_report(events_ids = [ event.id ] if event and event.id else None, users_ids = users_ids)
        
        if { 'Журнал' } & set(config.keys()):
            notes = await get_notes_for_report(users_ids = users_ids)

        for item in data:
            temp = dict(item).copy()

            # Активен
            if 'Активен' in config:
                if config['Активен']['report']:
                    temp.update({ 'active': 'Да' if item['active'] is True else 'Нет' })

            # Роль
            if 'Роль' in config:
                if config['Роль']['report']:
                    roles = {
                        'client': 'Клиент',
                        'applicant': 'Новичок',
                        'guest': 'Гость',
                    }
                    temp.update({ 'role': roles[item['role']] if item['role'] in roles else '' })

            # Стадия
            if 'Стадия' in config:
                tc = memberships[str(item['id'])]['stage']
                if config['Стадия']['filter']:
                    if config['Стадия']['value'] != '1000':
                        if int(config['Стадия']['value']) != tc:
                            continue
                if config['Стадия']['report']:
                    temp.update({ 'stage': STAGES[tc] })
            
            # Отказ
            if 'Отказ' in config:
                ts = memberships[str(item['id'])]['stage']
                rejection = memberships[str(item['id'])]['stages'][ts]['rejection']
                if config['Отказ']['filter']:
                    if config['Отказ']['value'] != '1000':
                        if config['Отказ']['value'] == '0' and rejection is True:
                            continue
                        if config['Отказ']['value'] == '1' and rejection is False:
                            continue
                if config['Отказ']['report']:
                    temp.update({ 'rejection': 'Да' if rejection else 'Нет' })

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
            
            # Пробный период (дней)
            if 'Пробный период (дней)' in config:
                ts = memberships[str(item['id'])]['stage']
                tc = memberships[str(item['id'])]['stages'][ts]['time']
                d = ''
                if tc:
                    dt1 = datetime.now().date()
                    dt2 = datetime.fromtimestamp(round(tc / 1000), pytz.utc).date()
                    d = (dt2 - dt1).days
                if config['Пробный период (дней)']['filter']:
                    if config['Пробный период (дней)']['value'] != '1000':
                        if ts not in { 4, 5 }:
                            continue
                        if not tc:
                            continue
                        if config['Пробный период (дней)']['value'] == '0' and d >= 0:
                            continue
                        if config['Пробный период (дней)']['value'] == '1' and d < 0:
                            continue
                if config['Пробный период (дней)']['report']:
                    temp.update({ 'demo': d })

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
            
            # Журнал
            if 'Журнал' in config:
                if config['Журнал']['report']:
                    feed = ''
                    if str(item['id']) in notes:
                        for note in notes[str(item['id'])]:
                            if note['time_update']:
                                dt = datetime.fromtimestamp(round(note['time_update'] / 1000), pytz.utc)
                            else:
                                dt = datetime.fromtimestamp(round(note['time_create'] / 1000), pytz.utc)
                            feed += note['author_name'] + ' | ' + '{:04d}'.format(dt.year) + '-' + '{:02d}'.format(dt.month) + '-' + '{:02d}'.format(dt.day) + ' ' + '{:02d}'.format(dt.hour) + ' ' + '{:02d}'.format(dt.minute) + "\n"
                            feed += note['text'] + "\n\n"
                    temp.update({ 'notes': feed })
                
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
                    if 'Мероприятие' not in config or config['Мероприятие']['value'] == '0':
                        if config['Присутствие на мероприятии']['value'] == '1000':
                            temp.update({ 'event_participation': 'Пришёл: ' + str(events_users_cache[str(item['id'])]['Пришёл']) + "\n" + 'Не пришёл: ' + str(events_users_cache[str(item['id'])]['Не пришёл']) })
                        elif config['Присутствие на мероприятии']['value'] == '0':
                            temp.update({ 'event_participation': 'Пришёл: ' + str(events_users_cache[str(item['id'])]['Не пришёл']) })
                        elif config['Присутствие на мероприятии']['value'] == '1':
                            temp.update({ 'event_participation': 'Пришёл: ' + str(events_users_cache[str(item['id'])]['Пришёл']) })
                    else:
                        if config['Присутствие на мероприятии']['value'] == '1000':
                            if events_users_cache[str(item['id'])]['Пришёл']:
                                temp.update({ 'event_participation': 'Пришёл' })
                            if events_users_cache[str(item['id'])]['Не пришёл']:
                                temp.update({ 'event_participation': 'Не пришёл' })
                        elif config['Присутствие на мероприятии']['value'] == '0':
                            if events_users_cache[str(item['id'])]['Не пришёл']:
                                temp.update({ 'event_participation': 'Не пришёл' })
                        elif config['Присутствие на мероприятии']['value'] == '1':
                            if events_users_cache[str(item['id'])]['Пришёл']:
                                temp.update({ 'event_participation': 'Пришёл' })
            
            if 'Назначенные встречи' in config:
                if config['Назначенные встречи']['report']:
                    temp.update({ 'connections': len(connections[str(item['id'])]['all']) if str(item['id']) in connections else 0 })
            
            if 'Состоявшиеся встречи' in config:
                if config['Состоявшиеся встречи']['report']:
                    temp.update({ 'connections_fulfilled': len(connections[str(item['id'])]['fulfilled']) if str(item['id']) in connections else 0 })
            
            if 'Коммьюнити-менеджер' in config:
                if config['Коммьюнити-менеджер']['report']:
                    temp.update({ 'community_manager': item['community_manager'] })
            
            result.append(temp)

    return result



################################################################
def create_clients_file(data):

    FIELDS = {
        'name': 'Имя',
        'role': 'Роль',
        'active': 'Активен',
        'company': 'Компания',
        'inn': 'ИНН',
        'catalog': 'Отрасль',
        'annual': 'Оборот',
        'position': 'Должность',
        'email': 'Email',
        'phone': 'Телефон',
        'link_telegram': 'Telegram ID',
        'stage': 'Стадия',
        'rejection': 'Отказ',
        'date_control': 'Контрольная дата',
        'demo': 'Пробный период (дней)',
        'notes': 'Журнал',
        'event': 'Мероприятие',
        'event_participation': 'Присутствие на мероприятии',
        'connections': 'Назначенные встречи',
        'connections_fulfilled': 'Состоявшиеся встречи',
        'time_last_activity': 'Активность',
        'community_manager': 'Коммьюнити-менеджер',
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
