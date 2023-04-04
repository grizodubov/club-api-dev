from app.core.context import get_api_context



####################################################################
class Item:


    ################################################################
    def __init__(self):
        self.id = 0
        self.model = None


    ################################################################
    async def set(self, id):
        api = get_api_context()
        if id:
            data = await api.pg.club.fetchrow(
                """SELECT
                        id, model
                    FROM
                        items_signatures
                    WHERE
                        id = $1""",
                id
            )
            self.__dict__ = dict(data)
