from app.core.context import get_api_context



async def get_tags_stats():
    api = get_api_context()
    data = await api.pg.club.fetch(
        """SELECT
                t1.id, t2.tags, t2.interests
            FROM
                users t1
            INNER JOIN
                users_tags t2 ON t2.user_id = t1.id
            WHERE
                t1.id >= 10000 AND
                t1.active IS TRUE"""
    )
    result = {
        'tags': {},
        'interests': {},
    }
    for row in data:
        for k in { 'tags', 'interests' }:
            if row[k]:
                tags = [ t.strip() for t in row[k].split(',') ]
                for t in tags:
                    if t:
                        if t in result[k]:
                            result[k][t] = result[k][t] + 1
                        else:
                            result[k][t] = 1
    return result



async def get_users_stats():
    api = get_api_context()
    data = await api.pg.club.fetchrow(
        """SELECT
                count(u.id) AS users,
                count(CASE WHEN 10002 = ANY(u.roles) THEN 1 END) AS clients,
                count(CASE WHEN 10001 = ANY(u.roles) THEN 1 END) AS managers,
                count(CASE WHEN now()::date = u.time_create::date THEN 1 END) AS users_new,
                count(CASE WHEN 10002 = ANY(u.roles) AND now()::date = u.time_create::date THEN 1 END) AS clients_new,
                count(CASE WHEN 10001 = ANY(u.roles) AND now()::date = u.time_create::date THEN 1 END) AS managers_new
            FROM
                (
                    SELECT
                        t1.id,
                        t1.time_create,
                        array_agg(t2.role_id) AS roles
                    FROM
                        users t1
                    INNER JOIN
                        users_roles t2 ON t2.user_id = t1.id
                    WHERE
                        t1.id >= 10000 AND
                        t1.active IS TRUE
                    GROUP BY
                        t1.id
                ) u"""
    )
    return {
        'users': data['users'],
        'clients': data['clients'],
        'managers': data['managers'],
        'users_new': data['users_new'],
        'clients_new': data['clients_new'],
        'managers_new': data['managers_new'],
    }
