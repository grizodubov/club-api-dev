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
        self.thumbs_up = 0

    
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
                        t1.detail, coalesce(t2.thumbs_up, 0) AS thumbs_up
                    FROM
                        events t1
                    LEFT JOIN
                        (SELECT item_id, count(user_id) AS thumbs_up FROM items_thumbs_up GROUP BY item_id) t2 ON t2.item_id = t1.id
                    WHERE
                        id = $1""",
                id
            )
            self.__dict__ = dict(data)
