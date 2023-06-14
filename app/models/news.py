import os.path

from app.core.context import get_api_context



####################################################################
class News:


    ################################################################
    def __init__(self):
        self.id = 0
        self.time_create = None
        self.time_update = None
        self.name = ''
        self.detail = ''
        self.time_news = None
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
                        t1.name, t1.detail, t1.time_news, t1.active,
                        coalesce(t2.thumbs_up, 0) AS thumbs_up
                    FROM
                        news t1
                    LEFT JOIN
                        (SELECT item_id, count(user_id) AS thumbs_up FROM items_thumbsup GROUP BY item_id) t2 ON t2.item_id = t1.id
                    """ + where + """ORDER BY t1.time_news DESC"""
        )
        for row in data:
            item = News()
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
                        t1.name, t1.detail, t1.time_news, t1.active,
                        coalesce(t2.thumbs_up, 0) AS thumbs_up
                    FROM
                        news t1
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
        for k in { 'active', 'name', 'detail', 'time_news' }:
            if k in kwargs:
                query.append(k + ' = $' + str(cursor))
                args.append(kwargs[k])
                cursor += 1
        if query:
            await api.pg.club.execute(
                """UPDATE
                        news
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
                    news (active, name, detail, time_news)
                VALUES
                    ($1, $2, $3, $4)
                RETURNING
                    id""",
            kwargs['active'],
            kwargs['name'],
            kwargs['detail'],
            kwargs['time_news']
        )
        await self.set(id = id)


    ################################################################
    def check_icon(self):
        self.icon = os.path.isfile('/var/www/static.clubgermes.ru/html/news/' + str(self.id) + '/icon.png')


    ################################################################
    def check_image(self):
        self.image = os.path.isfile('/var/www/static.clubgermes.ru/html/news/' + str(self.id) + '/img.jpg')


    ################################################################
    def check_files(self):
        self.files = os.path.isfile('/var/www/static.clubgermes.ru/html/news/' + str(self.id) + '/index.html') and \
                os.path.isfile('/var/www/static.clubgermes.ru/html/news/' + str(self.id) + '/menu.json')
