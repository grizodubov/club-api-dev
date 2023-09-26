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
                        coalesce(t2.thumbs_up, 0) AS thumbs_up
                    FROM
                        events t1
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
                        coalesce(t2.thumbs_up, 0) AS thumbs_up
                    FROM
                        events t1
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
