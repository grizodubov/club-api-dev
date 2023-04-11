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



####################################################################
class Items:


    ################################################################
    def __init__(self):
        self.list = []


    ################################################################
    async def set(self, ids):
        api = get_api_context()
        if ids:
            data = await api.pg.club.fetchrow(
                """SELECT
                        id, model
                    FROM
                        items_signatures
                    WHERE
                        id = ANY($1)""",
                ids
            )
            for row in data:
                item = Item()
                item.__dict__ = dict(row)
                self.list.append(item)


    ################################################################
    def model_check(self, model):
        result = True
        for item in self.list:
            if item.model != model:
                result = False
                break
        return result
