import uuid

from app.core.context import get_api_context



####################################################################
class Avatar:


    ################################################################
    def __init__(self):
        self.id = 0
        self.owner_id = 0
        self.owner_model = None
        self.time_create = None
        self.hash = ''
        self.active = False

    
    #################################################
    def show(self):
        return { k: v for k, v in self.__dict__.items() if not k.startswith('_') }


    ################################################################
    async def set(self, id = None, owner_id = None):
        api = get_api_context()
        if id and owner_id is None:
            data = await api.pg.club.fetchrow(
                """SELECT
                        id, owner_id, owner_model, time_create, hash, active
                    FROM
                        avatars
                    WHERE
                        id = $1""",
                id
            )
            self.__dict__ = dict(data)
        if id is None and owner_id:
            data = await api.pg.club.fetchrow(
                """SELECT
                        id, owner_id, owner_model, time_create, hash, active
                    FROM
                        avatars
                    WHERE
                        owner_id = $1 AND active IS TRUE""",
                owner_id
            )
            self.__dict__ = dict(data)


    ################################################################
    async def create(self, **kwargs):
        api = get_api_context()
        uid = ''
        generator = True
        while generator:
            uid = str(uuid.uuid4())
            temp = await api.pg.club.fetchval(
                """SELECT
                        id
                    FROM
                        avatars
                    WHERE
                        hash = $1""",
                uid
            )
            if not temp:
                generator = False
        if uid:
            id = await api.pg.club.fetchval(
                """INSERT INTO
                        avatars (owner_id, owner_model, hash, active)
                    VALUES
                        ($1, $2, $3, $4)
                    RETURNING
                        id""",
                kwargs['owner_id'], kwargs['owner_model'], uid, True
            )
            await api.pg.club.execute(
                """UPDATE
                        avatars
                    SET
                        active = False
                    WHERE
                        owner_id = $1 AND id <> $2
                    """,
                kwargs['owner_id'], id
            )
            await self.set(id = id)
