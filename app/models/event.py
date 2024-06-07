import os
import re
import orjson

from app.core.context import get_api_context



####################################################################
class Event:


    ################################################################
    def __init__(self):
        self.id = 0
        self.time_create = None
        self.time_update = None
        self.name = ''
        self.format = ''
        self.place = ''
        self.time_event = None
        self.detail = ''
        self.active = False
        self.thumbs_up = 0
        self.icon = False
        self.image = False
        self.files = False
        self.speakers = []
        self.program = []

    
    ################################################################
    @classmethod
    async def list(cls, active_only = False, start = None, finish = None, reverse = False):
        api = get_api_context()
        result = []
        where = []
        args = []
        i = 0
        if active_only:
            where.append('t1.active IS TRUE')
        if start:
            i += 1
            where.append('t1.time_event >= $' + str(i))
            args.append(start)
        if finish:
            i += 1
            where.append('t1.time_event <= $' + str(i))
            args.append(finish)
        where_query = ''
        if where:
            where_query = ' WHERE ' + ' AND '.join(where) + ' '
        # print(where_query)
        reverse_query = ''
        if reverse:
            reverse_query = ' DESC'
        data = await api.pg.club.fetch(
            """SELECT
                        t1.id, t1.time_create, t1.time_update,
                        t1.name, t1.format, t1.place, t1.time_event,
                        t1.detail, t1.active,
                        coalesce(t2.thumbs_up, 0) AS thumbs_up,
                        coalesce(t3.speakers, '{}'::jsonb[]) AS speakers,
                        coalesce(t4.program, '{}'::jsonb[]) AS program
                    FROM
                        events t1
                    LEFT JOIN
                        (
                            SELECT
                                s1.event_id, array_agg(jsonb_build_object('id', s1.user_id, 'name', s2.name)) AS speakers
                            FROM
                                events_speakers s1
                            INNER JOIN
                                users s2 ON s2.id = s1.user_id
                            GROUP BY
                                s1.event_id
                        ) t3 ON t3.event_id = t1.id
                    LEFT JOIN
                        (
                            SELECT
                                event_id, array_agg(jsonb_build_object('sort', sort, 'name', name, 'date', round(extract(epoch FROM date_item) * 1000), 'time', time_item, 'speakers', speakers) ORDER BY sort) AS program
                            FROM
                                events_programs
                            GROUP BY
                                event_id
                        ) t4 ON t4.event_id = t1.id
                    LEFT JOIN
                        (SELECT item_id, count(user_id) AS thumbs_up FROM items_thumbsup GROUP BY item_id) t2 ON t2.item_id = t1.id
                    """ + where_query + """ORDER BY t1.time_event""" + reverse_query,
            *args
        )
        for row in data:
            item = Event()
            item.__dict__ = dict(row)
            item.check_icon()
            item.check_image()
            item.check_files()
            result.append(item)
        return result


    ################################################################
    def show(self):
        return { k: v for k, v in self.__dict__.items() if not k.startswith('_') }



    ################################################################
    def get_patch(self):
        data = None
        patch = '/var/www/static.clubgermes.ru/html/events/' + str(self.id) + '/patch.json'
        if os.path.isfile(patch):
            with open(patch, 'rb') as openfile:
                try:
                    data = orjson.loads(openfile.read())
                except:
                    data = None
        return { '_patch': data }



    ################################################################
    def set_patch(self, patch):
        menu = [
            {
                "name": 'О мероприятии',
                "icon": "Information20",
            }
        ]
        html = [
            '<h1 id="О мероприятии" class="text-lg font-semibold">' + self.name + '</h1>'
        ]
        images = []
        if patch['blocks']:
            for block in patch['blocks']:
                if block['anchor']:
                    anchor = re.sub(r'[^\dA-Za-zАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдеёжзийклмнопрстуфхцчшщъыьэюя\.\,\(\)\-\_\!\&\*\%\$\# ]+', '', block['anchor'])
                    if anchor.strip():
                        menu.append({
                            "name": anchor.strip(),
                            "icon": block['icon'],
                        })
                        html.append('<h3 id="' + block['anchor'] + '" class="mt-6 font-semibold">' + block['anchor'] + '</h3>')
                if block['type'] == 'image':
                    html.append('<img class="rounded-xl w-full mt-6" src="https://static.clubgermes.ru/events/' + str(self.id) + '/patch/' + block['data'] + '.jpg" alt="" />')
                    images.append(block['data'] + '.jpg')
                else:
                    html.append('<div class="mt-5 text-sm">' + block['data'] + '</div>')
        # clear images
        dir = '/var/www/static.clubgermes.ru/html/events/' + str(self.id) + '/patch'
        for root, dirs, files in os.walk(dir):
            for name in files:
                if name not in images:
                    os.remove(os.path.join(root, name))
        html_file = '/var/www/static.clubgermes.ru/html/events/' + str(self.id) + '/index.html'
        with open(html_file, 'w', encoding='utf-8') as file:
            file.write('\n'.join(html))
        menu_file = '/var/www/static.clubgermes.ru/html/events/' + str(self.id) + '/menu.json'
        with open(menu_file, 'w', encoding='utf-8') as file:
            file.write(str(orjson.dumps(menu, option=orjson.OPT_INDENT_2), 'UTF-8'))
        patch_file = '/var/www/static.clubgermes.ru/html/events/' + str(self.id) + '/patch.json'
        with open(patch_file, 'w', encoding='utf-8') as file:
            file.write(str(orjson.dumps(patch, option=orjson.OPT_INDENT_2), 'UTF-8'))



    ################################################################
    async def set(self, id):
        api = get_api_context()
        if id:
            data = await api.pg.club.fetchrow(
                """SELECT
                        t1.id, t1.time_create, t1.time_update,
                        t1.name, t1.format, t1.place, t1.time_event,
                        t1.detail, t1.active,
                        coalesce(t2.thumbs_up, 0) AS thumbs_up,
                        coalesce(t3.speakers, '{}'::jsonb[]) AS speakers,
                        coalesce(t4.program, '{}'::jsonb[]) AS program
                    FROM
                        events t1
                    LEFT JOIN
                        (
                            SELECT
                                s1.event_id, array_agg(jsonb_build_object('id', s1.user_id, 'name', s2.name)) AS speakers
                            FROM
                                events_speakers s1
                            INNER JOIN
                                users s2 ON s2.id = s1.user_id
                            GROUP BY
                                s1.event_id
                        ) t3 ON t3.event_id = t1.id
                    LEFT JOIN
                        (
                            SELECT
                                event_id, array_agg(jsonb_build_object('sort', sort, 'name', name, 'date', round(extract(epoch FROM date_item) * 1000), 'time', time_item, 'speakers', speakers) ORDER BY sort) AS program
                            FROM
                                events_programs
                            GROUP BY
                                event_id
                        ) t4 ON t4.event_id = t1.id
                    LEFT JOIN
                        (SELECT item_id, count(user_id) AS thumbs_up FROM items_thumbsup GROUP BY item_id) t2 ON t2.item_id = t1.id
                    WHERE
                        id = $1""",
                id
            )
            self.__dict__ = dict(data)


    ################################################################
    async def update(self, **kwargs):
        api = get_api_context()
        cursor = 2
        query = []
        args = []
        for k in { 'active', 'name', 'format', 'place', 'time_event', 'detail' }:
            if k in kwargs:
                query.append(k + ' = $' + str(cursor))
                args.append(kwargs[k])
                cursor += 1
        if query:
            await api.pg.club.execute(
                """UPDATE
                        events
                    SET
                        """ + ', '.join(query) + """
                    WHERE
                        id = $1""",
                self.id, *args
            )


    ################################################################
    async def create(self, **kwargs):
        api = get_api_context()
        id = await api.pg.club.fetchval(
            """INSERT INTO
                    events (active, name, format, place, time_event, detail)
                VALUES
                    ($1, $2, $3, $4, $5, $6)
                RETURNING
                    id""",
            kwargs['active'],
            kwargs['name'],
            kwargs['format'],
            kwargs['place'],
            kwargs['time_event'],
            kwargs['detail']
        )
        await self.set(id = id)


    ################################################################
    async def info(self):
        api = get_api_context()
        info = self.show()
        return info


    ################################################################
    def check_icon(self):
        self.icon = os.path.isfile('/var/www/static.clubgermes.ru/html/events/' + str(self.id) + '/icon.png')


    ################################################################
    def check_image(self):
        self.image = os.path.isfile('/var/www/static.clubgermes.ru/html/events/' + str(self.id) + '/img.jpg')


    ################################################################
    def check_files(self):
        self.files = os.path.isfile('/var/www/static.clubgermes.ru/html/events/' + str(self.id) + '/index.html') and \
                os.path.isfile('/var/www/static.clubgermes.ru/html/events/' + str(self.id) + '/menu.json')
    

    ################################################################
    async def add_speaker(self, user_id):
        api = get_api_context()
        id = await api.pg.club.execute(
            """INSERT INTO
                    events_speakers (event_id, user_id)
                VALUES
                    ($1, $2)""",
            self.id, user_id
        )
        await self.set(id = self.id)
    

    ################################################################
    async def delete_speaker(self, user_id):
        api = get_api_context()
        id = await api.pg.club.execute(
            """DELETE FROM
                    events_speakers
                WHERE
                    event_id = $1 AND user_id = $2""",
            self.id, user_id
        )
        await self.set(id = self.id)


    ################################################################
    async def update_speakers(self, speakers):
        api = get_api_context()
        query = []
        args = [ self.id ]
        i = 2
        for speaker in speakers:
            query.append('($1, $' + str(i) + ')')
            args.append(speaker)
            i += 1
        await api.pg.club.execute(
            """DELETE FROM
                    events_speakers
                WHERE
                    event_id = $1 AND user_id <> ANY($2)""",
            self.id, speakers
        )
        await api.pg.club.execute(
            """INSERT INTO
                    events_speakers (event_id, user_id)
                VALUES
                    """ + ', '.join(query) + """ ON CONFLICT (event_id, user_id) DO NOTHING""",
            *args
        )
        await self.set(id = self.id)


    ################################################################
    async def update_program(self, program):
        api = get_api_context()
        query = []
        args = [ self.id ]
        j = 1
        i = 2
        for item in program:
            query.append('($1, $' + str(i) + ', $' + str(i + 1) + ', $' + str(i + 2) + ', $' + str(i + 3) + ', $' + str(i + 4) + ')')
            args.extend([ item['name'], item['speakers'], item['date'], item['time'], j ])
            i += 5
            j += 1
        await api.pg.club.execute(
            """DELETE FROM
                    events_programs
                WHERE
                    event_id = $1""",
            self.id
        )
        await api.pg.club.execute(
            """INSERT INTO
                    events_programs (event_id, name, speakers, date_item, time_item, sort)
                VALUES
                    """ + ', '.join(query),
            *args
        )
        await self.set(id = self.id)



################################################################
async def find_closest_event(mark):
    api = get_api_context()
    data = await api.pg.club.fetchval(
        """SELECT
                time_event
            FROM
                events
            WHERE
                time_event > $1
            ORDER BY
                time_event
            LIMIT 1""",
        mark
    )
    if not data:
        data = await api.pg.club.fetchval(
            """SELECT
                    time_event
                FROM
                    events
                WHERE
                    time_event < $1
                ORDER BY
                    time_event DESC
                LIMIT 1""",
            mark
        )
    return data



################################################################
async def get_participants(events_ids):
    api = get_api_context()
    data = await api.pg.club.fetch(
        """SELECT
                t1.event_id,
                array_agg(jsonb_build_object('id', t1.user_id, 'name', t2.name, 'confirmation', t1.confirmation, 'audit', t1.audit)) AS participants
            FROM
                events_users t1
            INNER JOIN
                users t2 ON t2.id = t1.user_id
            WHERE
                t1.event_id = ANY($1)
            GROUP BY
                t1.event_id""",
        events_ids
    )
    return { str(item['event_id']): item['participants'] for item in data }



################################################################
async def get_participants_with_avatars(events_ids):
    api = get_api_context()
    data = await api.pg.club.fetch(
        """SELECT
                t1.event_id,
                array_agg(jsonb_build_object(
                    'id', t1.user_id,
                    'name', t2.name,
                    'company', t22.company,
                    'confirmation', t1.confirmation,
                    'audit', t1.audit,
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
                t1.event_id = ANY($1)
            GROUP BY
                t1.event_id""",
        events_ids
    )
    return { str(item['event_id']): item['participants'] for item in data }



################################################################
async def get_all_speakers():
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
                t3.alias = $1
            ORDER BY
                t1.name""",
        'speaker'
    )
    return [ dict(item) for item in data ]



################################################################
async def get_speakers(events_ids):
    api = get_api_context()
    data = await api.pg.club.fetch(
        """SELECT
                t1.event_id,
                array_agg(jsonb_build_object(
                    'id', t1.user_id,
                    'name', t2.name,
                    'company', t22.company,
                    'confirmation', true,
                    'audit', 2,
                    'avatar_hash', t5.hash,
                    'tags', '',
                    'interests', '',
                    'tags_event','',
                    'interests_event', '',
                    'community_manager_id', t2.community_manager_id,
                    'community_manager', coalesce(t4.name, '')
                )) AS speakers
            FROM
                events_speakers t1
            INNER JOIN
                users t2 ON t2.id = t1.user_id
            INNER JOIN
                users_info t22 ON t22.user_id = t2.id
            LEFT JOIN
                users t4 ON t4.id = t2.community_manager_id
            LEFT JOIN
                avatars t5 ON t5.owner_id = t2.id AND t5.active IS TRUE
            WHERE
                t1.event_id = ANY($1)
            GROUP BY
                t1.event_id""",
        events_ids
    )
    return { str(item['event_id']): item['speakers'] for item in data }



################################################################
async def get_events_confirmations_pendings(users_ids = None):
    api = get_api_context()
    query = ''
    args = []
    if users_ids:
        query += ' AND t2.user_id = ANY($1)'
        args.append(users_ids)
    result = await api.pg.club.fetch(
        """SELECT
                t2.user_id, count(t1.id) AS amount
            FROM
                events t1
            INNER JOIN
                events_users t2 ON t2.event_id = t1.id
            INNER JOIN
                users_roles t3 ON t3.user_id = t2.user_id
            INNER JOIN
                roles t4 ON t4.id = t3.role_id
            WHERE
                t1.active IS TRUE AND
                t1.time_event >= (now() at time zone 'utc')::date AND
                t2.confirmation IS FALSE AND
                t4.alias = 'client'""" + query + """
            GROUP BY
                t2.user_id""",
       *args
    )
    return {
        str(item['user_id']): item['amount'] for item in result
    }



################################################################
async def get_future_events():
    api = get_api_context()
    data = await api.pg.club.fetch(
        """SELECT
                t1.id, t1.name, t1.format, t1.place, t1.time_event, t1.detail, FALSE AS archive
            FROM
                events t1
            WHERE
                t1.active IS TRUE AND
                t1.time_event >= (now() at time zone 'utc')::date
            ORDER BY
                t1.time_event"""
    )
    return [ dict(item) for item in data ]



################################################################
async def get_events():
    api = get_api_context()
    data = await api.pg.club.fetch(
        """SELECT
                t1.id, t1.name, t1.format, t1.place, t1.time_event, t1.detail, FALSE AS archive
            FROM
                events t1
            WHERE
                t1.active IS TRUE AND
                t1.time_event >= '2024-06-05'::date
            ORDER BY
                t1.time_event"""
    )
    return [ dict(item) for item in data ]
