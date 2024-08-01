from datetime import datetime

from app.core.context import get_api_context



####################################################################
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



####################################################################
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



####################################################################
async def get_new_clients_stats(dt):
    api = get_api_context()
    data = await api.pg.club.fetchrow(
        """SELECT
                count(*) AS clients_all,
                count(*) FILTER (WHERE t1.time_create > date($1) AND t1.time_create < date($1) + interval '24 hours') AS clients_yersterday,
                count(*) FILTER (WHERE t1.time_create > date($1) - interval '144 hours' AND t1.time_create < date($1) + interval '24 hours') AS clients_week,
                count(*) FILTER (WHERE t1.time_create > date($1) - interval '696 hours' AND t1.time_create < date($1) + interval '24 hours') AS clients_month
            FROM
                users t1
            INNER JOIN
                users_roles t2 ON t2.user_id = t1.id
            INNER JOIN
                roles t3 ON t3.id = t2.role_id
            WHERE
                t3.alias = 'client'""",
        datetime.strptime(dt, '%Y-%m-%d').date()
    )
    return {
        'clients_all': data['clients_all'],
        'clients_yersterday': data['clients_yersterday'],
        'clients_week': data['clients_week'],
        'clients_month': data['clients_month'],
    }



####################################################################
async def get_signings_stats(dt):
    api = get_api_context()
    data = await api.pg.club.fetchrow(
        """SELECT
                count(DISTINCT t1.user_id) AS signings_all,
                count(DISTINCT t1.user_id) FILTER (WHERE t1.time_sign > date($1) AND t1.time_sign < date($1) + interval '24 hours') AS signings_yersterday,
                count(DISTINCT t1.user_id) FILTER (WHERE t1.time_sign > date($1) - interval '144 hours' AND t1.time_sign < date($1) + interval '24 hours') AS signings_week,
                count(DISTINCT t1.user_id) FILTER (WHERE t1.time_sign > date($1) - interval '696 hours' AND t1.time_sign < date($1) + interval '24 hours') AS signings_month
            FROM
                signings t1
            INNER JOIN
                users_roles t2 ON t2.user_id = t1.user_id
            INNER JOIN
                roles t3 ON t3.id = t2.role_id
            WHERE
                t3.alias = 'client'""",
        datetime.strptime(dt, '%Y-%m-%d').date()
    )
    return {
        'signings_all': data['signings_all'],
        'signings_yersterday': data['signings_yersterday'],
        'signings_week': data['signings_week'],
        'signings_month': data['signings_month'],
    }



####################################################################
async def get_unique_views_stats(dt):
    api = get_api_context()
    data = await api.pg.club.fetchrow(
        """SELECT
                count(DISTINCT concat(t1.user_id::text, ' ', t1.target_id::text)) AS views_all_unique,
                count(DISTINCT concat(t1.user_id::text, ' ', t1.target_id::text)) FILTER (WHERE t1.time_view > date($1) AND t1.time_view < date($1) + interval '24 hours') AS views_yersterday_unique,
                count(DISTINCT concat(t1.user_id::text, ' ', t1.target_id::text)) FILTER (WHERE t1.time_view > date($1) - interval '144 hours' AND t1.time_view < date($1) + interval '24 hours') AS views_week_unique,
                count(DISTINCT concat(t1.user_id::text, ' ', t1.target_id::text)) FILTER (WHERE t1.time_view > date($1) - interval '696 hours' AND t1. time_view < date($1) + interval '24 hours') AS views_month_unique
            FROM
                users_profiles_views t1
            INNER JOIN
                users_roles t2 ON t2.user_id = t1.user_id
            INNER JOIN
                roles t3 ON t3.id = t2.role_id
            WHERE
                t3.alias = 'client'""",
        datetime.strptime(dt, '%Y-%m-%d').date()
    )
    return {
        'views_all': data['views_all_unique'],
        'views_yersterday': data['views_yersterday_unique'],
        'views_week': data['views_week_unique'],
        'views_month': data['views_month_unique'],
    }
