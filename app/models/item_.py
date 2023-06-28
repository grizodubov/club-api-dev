from app.core.context import get_api_context



####################################################################
class Items:


    ################################################################
    def __init__(self):
        self.model = None
        self.items = []


    ################################################################
    async def search(self, model, filter = None, sort = None, offset = None, limit = None):
        api = get_api_context()
        data = await api.pg.club.fetchrow(
            """SELECT
                    storage, links, merges
                FROM
                    models
                WHERE
                    model = $1""",
            model
        )
        if data:
            self.model = model
            storages = [ data['storage'] ]
            columns = 't1.*'
            tables = data['storage'] + ' t1'
            args = []
            if data['merges']:
                storages.extend([ merge['storage'] for merge in data['merges'] ])
                for i, merge in enumerate(data['merges'], 2):
                    if merge['many']:
                        columns += ", array_agg(to_jsonb(t" + str(i) + ".*) - '" + merge['field'] + "') AS " + merge['storage']
                    else:
                        columns += ', t' + str(i) + '.*'
                    if merge['strict']:
                        tables += ' INNER JOIN ' + merge['storage'] + ' t' + str(i)
                    else:
                        tables += ' LEFT JOIN ' + merge['storage'] + ' t' + str(i)
            query = 'SELECT ' + columns + ' FROM ' + tables
            args = []
            if filter:
                where = []
                i = 1
                for k, v in filter.items():
                    for sample in [ '<>', '<=', '>=', '<', '>', '=' ]:
                        if k.endswith(sample):
                            k = k[:-1 * len(sample)]
                            if v is None:
                                if sample == '<>':
                                    where.append(prefix + k + ' IS NOT NULL')
                                else:
                                    where.append(prefix + k + ' IS NULL')
                            else:
                                if isinstance(v, list):
                                    where.append(prefix + k + ' ' + sample + ' ANY($' + str(i) + ')')
                                else:
                                    where.append(prefix + k + ' ' + sample + ' $' + str(i))
                                args.append(v)
                                i += 1
                if where:
                    query += ' WHERE ' + ' AND '.join(where)
            if sort:
                order = []
                if isinstance(sort, list):
                    for k in sort:
                        if k.endswith('!'):
                            k = k[:-1]
                            order.append(k + ' DESC')
                        else:
                            order.append(k)
                else:
                    if sort.endswith('!'):
                        k = sort[:-1]
                        order.append(k + ' DESC')
                    else:
                        order.append(sort)
                query += ' ORDER BY ' + ', '.join(order)
            if offset is not None:
                query += ' OFFSET $' + str(i)
                args.append(offset)
                i += 1
            if limit is not None:
                query += ' LIMIT $' + str(i)
                args.append(limit)
                i += 1
            result = await api.pg.club.fetch(query, *args)
            self.items = [ dict(item) for item in result ]
