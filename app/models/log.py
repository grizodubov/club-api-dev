from app.core.context import get_api_context



################################################################
async def get_sign_log(page = 1, roles = None, community_manager_id = None):
    api = get_api_context()
    offset = (page - 1) * 25
    if not roles:
        roles = await api.pg.club.fetchval("""SELECT array_agg(alias) FROM roles WHERE id <> 10000""")
    query = ''
    args = []
    if community_manager_id:
        users_ids = await api.pg.club.fetchval("""SELECT array_agg(id) FROM users WHERE community_manager_id = $1""", community_manager_id)
        if not users_ids:
            return (0, [])
        query = 'r1.user_id = ANY($1) AND'
        args.append(list(users_ids))
    elif community_manager_id == 0:
        users_ids = await api.pg.club.fetchval(
            """SELECT
                    array_agg(t1.id)
                FROM
                    users t1
                INNER JOIN users_roles t2 ON t2.user_id = t1.id
                INNER JOIN roles t3 ON t3.id = t2.role_id
                WHERE
                    t1.community_manager_id IS NULL AND t3.alias = 'client'"""
        )
        if not users_ids:
            return (0, [])
        query = 'r1.user_id = ANY($1) AND'
        args.append(list(users_ids))
    args.append(roles)
    amount = await api.pg.club.fetchval(
        """SELECT
                count(*)
            FROM
            (
                SELECT
                    session_id, user_id
                FROM
                    signings
                WHERE
                    sign_in IS TRUE AND
                    time_sign > now() at time zone 'utc' - interval \'35 days\'
            ) AS t1
            INNER JOIN
                (
                    SELECT
                        r1.user_id
                    FROM
                        users_roles r1
                    INNER JOIN
                        roles r2 ON r2.id = r1.role_id
                    WHERE
                        r1.user_id >= 10000 AND
                        """ + query + """
                        r2.alias = ANY($""" + str(len(args)) + """)
                    GROUP BY
                        r1.user_id
                ) t3 ON t3.user_id = t1.user_id""",
        *args
    )
    args.append(offset)
    data = await api.pg.club.fetch(
        """
            SELECT
                t1.*, t3.roles, t2.name, t6.hash AS avatar_hash, t7.settings
            FROM
            (
                SELECT
                    r1.session_id, r1.user_id, r1.time_sign AS time_from, min(r2.time_sign) AS time_to
                FROM
                    signings r1
                LEFT JOIN
                    signings r2 ON r2.session_id = r1.session_id AND r2.time_sign > r1.time_sign AND r2.sign_in IS FALSE
                WHERE
                    r1.sign_in IS TRUE AND
                    r1.time_sign > now() at time zone 'utc' - interval \'35 days\'
                GROUP BY
                    r1.session_id, r1.user_id, r1.time_sign
            ) AS t1
            INNER JOIN
                users t2 ON t2.id = t1.user_id
            INNER JOIN
                sessions t7 ON t7.id = t1.session_id
            INNER JOIN
                (
                    SELECT
                        r1.user_id, array_agg(r2.alias) AS roles
                    FROM
                        users_roles r1
                    INNER JOIN
                        roles r2 ON r2.id = r1.role_id
                    WHERE
                        r1.user_id >= 10000 AND
                        """ + query + """
                        r2.alias = ANY($""" + str(len(args) - 1) + """)
                    GROUP BY
                        r1.user_id
                ) t3 ON t3.user_id = t1.user_id
            LEFT JOIN
                avatars t6 ON t6.owner_id = t1.user_id AND t6.active IS TRUE
            ORDER BY
                t1.time_to DESC NULLS FIRST, t1.time_from DESC
            OFFSET
                $""" + str(len(args)) + """
            LIMIT
                25""",
        *args
    )
    return (amount, [ dict(item) for item in data ])



################################################################
async def get_views(page = 1, community_manager_id = None):
    api = get_api_context()
    offset = (page - 1) * 50
    where = ''
    args = []
    if community_manager_id and community_manager_id > 0:
        users_ids = await api.pg.club.fetchval("""SELECT array_agg(id) FROM users WHERE community_manager_id = $1""", community_manager_id)
        if not users_ids:
            return (0, [])
        where = 't1.user_id = ANY($1) OR t1.target_id = ANY($1)'
        args.append(list(users_ids))
    elif community_manager_id == 0:
        users_ids = await api.pg.club.fetchval("""SELECT array_agg(id) FROM users WHERE community_manager_id IS NULL""")
        if not users_ids:
            return (0, [])
        where = 't1.user_id = ANY($1) OR t1.target_id = ANY($1)'
        args.append(list(users_ids))
    else:
        users_ids = await api.pg.club.fetchval("""SELECT array_agg(user_id) FROM users_roles WHERE role_id = 10002""")
        if not users_ids:
            return (0, [])
        where = 't1.user_id = ANY($1) AND t1.target_id = ANY($1)'
        args.append(list(users_ids))
    amount = await api.pg.club.fetchval(
        """SELECT
                count(*)
            FROM
                users_profiles_views t1
            WHERE """ + where,
        *args
    )
    args.append(offset)
    data = await api.pg.club.fetch(
        """SELECT
                t1.time_view,
                t2.id AS user_id, t2.name AS user_name, t3.company AS user_company, t2.active AS user_active, t4.hash AS user_avatar_hash,
                t5.id AS target_id, t5.name AS target_name, t6.company AS target_company, t5.active AS target_active, t7.hash AS target_avatar_hash
            FROM
                users_profiles_views t1
            INNER JOIN
                users t2 ON t2.id = t1.user_id
            INNER JOIN
                users_info t3 ON t3.user_id = t1.user_id
            INNER JOIN
                users t5 ON t5.id = t1.target_id
            INNER JOIN
                users_info t6 ON t6.user_id = t1.target_id
            LEFT JOIN
                avatars t4 ON t4.owner_id = t1.user_id AND t4.active IS TRUE
            LEFT JOIN
                avatars t7 ON t7.owner_id = t1.target_id AND t7.active IS TRUE
            WHERE """ + where + """
            ORDER BY
                t1.time_view DESC
            OFFSET
                $""" + str(len(args)) + """
            LIMIT
                50""",
        *args
    )
    return (amount, [ dict(item) for item in data ])
