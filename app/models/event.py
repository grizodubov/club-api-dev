import os.path

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
    async def list(cls, active_only = False):
        api = get_api_context()
        result = []
        where = ''
        if active_only:
            where = ' WHERE t1.active IS TRUE '
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
                    """ + where + """ORDER BY t1.time_event DESC"""
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
