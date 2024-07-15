from app.core.context import get_api_context



####################################################################
async def get_list(user_id, time_breakpoint = None, limit = None):
    api = get_api_context()
    where = [ 'user_id = $1' ]
    query = ''
    args = [ user_id ]
    if time_breakpoint:
        if limit is None:
            where.append('time_notify >= $2')
            args.append(time_breakpoint)
        else:
            where.append('time_notify < $2')
            args.append(time_breakpoint)
            query = 'LIMIT $3'
            args.append(limit)
    else:
        if limit is None:
            limit = 20
            query = 'LIMIT $3'
            args.append(limit)
    result = await api.pg.club.fetch(
        """SELECT
                time_notify::text AS time_notify, time_view, event, data
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
async def get_stats(user_id):
    api = get_api_context()
    result = await api.pg.club.fetchrow(
        """SELECT
                count(time_notify) AS all,
                count(time_notify) FILTER (WHERE time_view IS NULL) AS new
            FROM
                notifications_1
            WHERE
                user_id = $1""",
        user_id
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
async def create(user_id, event, data):
    api = get_api_context()
    await api.pg.club.execute(
        """INSERT INTO
                notifications_1 (user_id, event, data)
            VALUES
                ($1, $2, $3)""",
        user_id, event, data
    )
