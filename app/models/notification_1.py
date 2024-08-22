from app.core.context import get_api_context



####################################################################
async def get_list(user_id, time_breakpoint = None, limit = None, mode = 'client'):
    api = get_api_context()
    where = [ 'user_id = $1', 'mode = $2' ]
    query = ''
    args = [ user_id, mode ]
    if time_breakpoint:
        if limit is None:
            where.append('time_notify >= $3')
            args.append(time_breakpoint)
        else:
            where.append('time_notify < $3')
            args.append(time_breakpoint)
            query = 'LIMIT $4'
            args.append(limit)
    else:
        if limit is None:
            limit = 20
            query = 'LIMIT $3'
            args.append(limit)
    result = await api.pg.club.fetch(
        """SELECT
                time_notify, time_notify::text AS time_notify_key, time_view, event, data
            FROM
                notifications_1
            WHERE
                """ + ' AND '.join(where) + """
            ORDER BY
                time_notify DESC
            """ + query,
        *args
    )
    if result:
        return [ dict(item) for item in result ]
    return []



####################################################################
async def get_stats(user_id, mode = None):
    api = get_api_context()
    query = ''
    args = [ user_id ]
    if mode:
        query = ' AND mode = $2'
        args.append(mode)
    result = await api.pg.club.fetchrow(
        """SELECT
                count(time_notify) AS all,
                count(time_notify) FILTER (WHERE time_view IS NULL) AS new
            FROM
                notifications_1
            WHERE
                user_id = $1""" + query,
        *args
    )
    return {
        'all': result['all'],
        'new': result['new'],
    }



####################################################################
async def view(user_id, time_notify = None):
    api = get_api_context()
    where = [ 'user_id = $1' ]
    args = [ user_id ]
    if time_notify is not None:
        where.append('time_notify = $2')
        args.append(time_notify)
    await api.pg.club.execute(
        """UPDATE
                notifications_1
            SET
                time_view = now() at time zone 'utc'
            WHERE
                """ + ' AND '.join(where),
        *args
    )



####################################################################
async def create(user_id, event, data, mode = 'client'):
    api = get_api_context()
    await api.pg.club.execute(
        """INSERT INTO
                notifications_1 (user_id, event, data, mode)
            VALUES
                ($1, $2, $3, $4)""",
        user_id, event, data, mode
    )



####################################################################
async def create_multiple(users_ids, event, data, mode = 'client'):
    api = get_api_context()
    i = 1
    query = []
    args = []
    for id in users_ids:
        temp = '($' + str(i) + ', $' + str(i + 1) + ', $' + str(i + 2) + ', $' + str(i + 3) + ')'
        query.append(temp)
        args.extend([ id, event, data, mode ])
        i = i + 4
    await api.pg.club.execute(
        """INSERT INTO
                notifications_1 (user_id, event, data, mode)
            VALUES
                """ + ', '.join(query),
        *args
    )



####################################################################
async def get_connections(events_ids, user_id):
    api = get_api_context()
    data = await api.pg.club.fetch(
        """SELECT
                event_id, user_1_id, user_2_id, response
            FROM
                users_connections
            WHERE
                event_id = ANY($1) AND
                (user_1_id = $2 OR user_2_id = $2) AND
                deleted IS FALSE""",
        events_ids, user_id
    )
    result = {}
    for item in data:
        if str(item['event_id']) not in result:
            result[str(item['event_id'])] = {}
        u = item['user_1_id'] if item['user_1_id'] != user_id else item['user_2_id']
        result[str(item['event_id'])][str(u)] = { 'response': item['response'] }
    return result
