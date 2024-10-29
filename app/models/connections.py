import asyncio
import math

from app.core.context import get_api_context



################################################################
async def get_connections_list(page = 1, community_manager_id = None, state = None, form = 'all', evaluation = [ True, True, True ], date_creation = None):
    api = get_api_context()
    args = []
    i = 1
    where = [ 't1.deleted IS FALSE' ]
    if community_manager_id is not None:
        if community_manager_id == 0:
            where.append('(u1_1.community_manager_id IS NULL OR u2_1.community_manager_id IS NULL)')
        else:
            args.append(community_manager_id)
            where.append('(u1_1.community_manager_id = $' + str(len(args)) + ' OR u2_1.community_manager_id = $' + str(len(args)) + ')')
    if state is not None:
        args.append(state)
        where.append('t1.state = $' + str(len(args)))
    if form == 'event':
        where.append('t1.event_id IS NOT NULL')
    elif form == 'contact':
        where.append('t1.event_id IS NULL')
    if date_creation:
        args.append(date_creation)
        where.append("""t1.time_create >= ((now() at time zone 'utc')::date - make_interval(days => ${t}))""".format(t = len(args)))
    if not evaluation[0] or not evaluation[1] or not evaluation[2]:
        if evaluation[0] and not evaluation[1] and not evaluation[2]:
            where.append('(t1.user_rating_1 IS NOT NULL AND t1.user_rating_2 IS NOT NULL)')
        if not evaluation[0] and evaluation[1] and not evaluation[2]:
            where.append('((t1.user_rating_1 IS NOT NULL AND t1.user_rating_2 IS NULL) OR (t1.user_rating_1 IS NULL AND t1.user_rating_2 IS NOT NULL))')
        if not evaluation[0] and not evaluation[1] and evaluation[2]:
            where.append('(t1.user_rating_1 IS NULL AND t1.user_rating_2 IS NULL)')
        if evaluation[0] and evaluation[1] and not evaluation[2]:
            where.append('(t1.user_rating_1 IS NOT NULL OR t1.user_rating_2 IS NOT NULL)')
        if evaluation[0] and not evaluation[1] and evaluation[2]:
            where.append('((t1.user_rating_1 IS NOT NULL AND t1.user_rating_2 IS NOT NULL) OR (t1.user_rating_1 IS NULL AND t1.user_rating_2 IS NULL))')
        if not evaluation[0] and evaluation[1] and evaluation[2]:
            where.append('(t1.user_rating_1 IS NULL OR t1.user_rating_2 IS NULL)')
        if not evaluation[0] and not evaluation[1] and not evaluation[2]:
            where.append('FALSE')
    amount = await api.pg.club.fetchrow(
        """SELECT
                count(*) AS all
            FROM
                users_connections t1
            INNER JOIN
                users u1_1 ON u1_1.id = t1.user_1_id
            INNER JOIN
                users u2_1 ON u2_1.id = t1.user_2_id
            WHERE
                """ + ' AND '.join(where),
        *args
    )
    all = []
    if amount['all']:
        last_page = math.floor(amount['all'] / 25)
        if amount['all'] % 25:
            last_page = last_page + 1
        if page > last_page:
            page = last_page
        offset = (page - 1) * 25
        args.append(offset)
        data_connections = await api.pg.club.fetch(
            """SELECT
                    t1.id, t1.event_id, t1.user_1_id, t1.user_2_id, u1_1.active AS user_1_active, u2_1.active AS user_2_active,
                    t1.state, t1.creator_id, t1.rating_1, t1.rating_2,
                    t1.user_rating_1, t1.user_rating_2,
                    t1.response, t1.response_1, t1.response_2,
                    t1.time_create, t1.user_comment_1, t1.user_comment_2,
                    u1_1.name AS user_1, u1_2.company AS user_1_company, u1_3.hash AS avatar_1_hash,
                    u2_1.name AS user_2, u2_2.company AS user_2_company, u2_3.hash AS avatar_2_hash,
                    m1_1.id AS community_manager_1_id, m1_1.name AS community_manager_1,
                    m2_1.id AS community_manager_2_id, m2_1.name AS community_manager_2,
                    t1.time_create, t1.time_user_rating_1, t1.time_user_rating_2
                FROM
                    users_connections t1
                INNER JOIN
                    users u1_1 ON u1_1.id = t1.user_1_id
                INNER JOIN
                    users_info u1_2 ON u1_2.user_id = u1_1.id
                INNER JOIN
                    users u2_1 ON u2_1.id = t1.user_2_id
                INNER JOIN
                    users_info u2_2 ON u2_2.user_id = u2_1.id
                LEFT JOIN
                    avatars u1_3 ON u1_3.owner_id = u1_1.id AND u1_3.active IS TRUE
                LEFT JOIN
                    avatars u2_3 ON u2_3.owner_id = u2_1.id AND u2_3.active IS TRUE
                LEFT JOIN
                    users m1_1 ON m1_1.id = u1_1.community_manager_id
                LEFT JOIN
                    users m2_1 ON m2_1.id = u2_1.community_manager_id
                WHERE
                    """ + ' AND '.join(where) + """
                ORDER BY
                    t1.id DESC
                OFFSET
                    ${v}
                LIMIT
                    25""".format(v = len(args)),
            *args
        )
        if data_connections:
            all = [ dict(item) for item in data_connections ]
    return (amount['all'], all, page)
