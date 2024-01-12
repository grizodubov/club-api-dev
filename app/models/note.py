from app.core.context import get_api_context



####################################################################
class Note:


    ################################################################
    def __init__(self):
        self.id = 0
        self.time_create = None
        self.time_update = None
        self.user_id = None
        self.author_id = None
        self.author_id_deleted = None
        self.author_name = ''
        self.note = ''
    

    ################################################################
    @classmethod
    async def list(cls, user_id):
        api = get_api_context()
        result = []
        data = await api.pg.club.fetch(
            """SELECT
                    t1.id,
                    round(extract(epoch FROM t1.time_create) * 1000)::bigint AS time_create,
                    round(extract(epoch FROM t1.time_update) * 1000)::bigint AS time_update,
                    t1.author_id, t1.author_id_deleted, t2.name AS author_name,
                    t1.note, t1.user_id
                FROM
                    notes t1
                INNER JOIN
                    users t2 ON t2.id = t1.author_id OR t2.id = t1.author_id_deleted
                WHERE
                    t1.user_id = $1
                ORDER BY t1.time_create""",
            user_id
        )
        for row in data:
            item = Note()
            item.__dict__ = dict(row)
            result.append(item)
        return result

    
    ################################################################
    def reset(self):
        self.__init__()


    ################################################################
    def show(self):
        return { k: v for k, v in self.__dict__.items() if not k.startswith('_') }


    ################################################################
    async def set(self, id):
        api = get_api_context()
        if id:
            data = await api.pg.club.fetchrow(
                """SELECT
                        t1.id,
                        round(extract(epoch FROM t1.time_create) * 1000)::bigint AS time_create,
                        round(extract(epoch FROM t1.time_update) * 1000)::bigint AS time_update,
                        t1.author_id, t1.author_id_deleted, t2.name AS author_name,
                        t1.note, t1.user_id
                    FROM
                        notes t1
                    INNER JOIN
                        users t2 ON t2.id = t1.author_id OR t2.id = t1.author_id_deleted
                    WHERE
                        t1.id = $1""",
                id
            )
            self.__dict__ = dict(data)
    

    ################################################################
    async def create(self, note, user_id, author_id):
        api = get_api_context()
        id = await api.pg.club.fetchval(
            """INSERT INTO
                    notes (note, user_id, author_id)
                VALUES
                    ($1, $2, $3)
                RETURNING
                    id""",
            note, user_id, author_id
        )
        await self.set(id = id)


    ################################################################
    async def update(self, note):
        api = get_api_context()
        if not note:
            await api.pg.club.execute(
                """DELETE FROM
                        notes
                    WHERE
                        id = $1""",
                self.id
            )
            self.reset()
        else:
            await api.pg.club.execute(
                """UPDATE
                        notes
                    SET
                        note = $2,
                        time_update = now() at time zone 'utc'
                    WHERE
                        id = $1""",
                self.id, note
            )
            await self.set(id = self.id)
