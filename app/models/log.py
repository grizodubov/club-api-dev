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
        query = 't3.user_id = ANY($1) AND'
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
        query = 't3.user_id = ANY($1) AND'
        args.append(list(users_ids))
    args.append(roles)
    amount = await api.pg.club.fetchval(
        """SELECT
                count(*)
            FROM
                signings t1
            INNER JOIN
                (
                    SELECT
                        r3.user_id, array_agg(r3.alias) AS roles
                    FROM
                        (
                            SELECT
                                r1.user_id, r2.alias
                            FROM
                                users_roles r1
                            INNER JOIN
                                roles r2 ON r2.id = r1.role_id
                        ) r3
                    GROUP BY
                        r3.user_id
                ) t3 ON t3.user_id = t1.user_id
            LEFT JOIN LATERAL
                (
                    SELECT
                        *
                    FROM (
                        SELECT *, row_number() OVER (
                            PARTITION BY session_id
                            ORDER BY time_sign
                        ) AS row_num
                        FROM
                            signings t5
                        WHERE t5.time_sign > t1.time_sign AND t5.sign_in IS FALSE
                        
                    ) AS ordered_signings
                    WHERE ordered_signings.row_num = 1
                ) t4 ON t4.session_id = t1.session_id
            WHERE
                t1.user_id >= 10000 AND
                t1.sign_in IS TRUE AND
                """ + query + """
                t3.roles && $""" + str(len(args)),
        *args
    )
    args.append(offset)
    data = await api.pg.club.fetch(
        """SELECT
                t1.session_id, t1.user_id, t1.sign_in, t1.time_sign AS time_from, t2.name, t3.roles, t4.time_sign AS time_to, t6.hash AS avatar_hash, t7.settings
            FROM
                signings t1
            INNER JOIN
                users t2 ON t2.id = t1.user_id
            INNER JOIN
                sessions t7 ON t7.id = t1.session_id
            INNER JOIN
                (
                    SELECT
                        r3.user_id, array_agg(r3.alias) AS roles
                    FROM
                        (
                            SELECT
                                r1.user_id, r2.alias
                            FROM
                                users_roles r1
                            INNER JOIN
                                roles r2 ON r2.id = r1.role_id
                        ) r3
                    GROUP BY
                        r3.user_id
                ) t3 ON t3.user_id = t1.user_id
            LEFT JOIN LATERAL
                (
                    SELECT
                        *
                    FROM (
                        SELECT *, row_number() OVER (
                            PARTITION BY session_id
                            ORDER BY time_sign
                        ) AS row_num
                        FROM
                            signings t5
                        WHERE t5.time_sign > t1.time_sign AND t5.sign_in IS FALSE
                        
                    ) AS ordered_signings
                    WHERE ordered_signings.row_num = 1
                ) t4 ON t4.session_id = t1.session_id
            LEFT JOIN
                avatars t6 ON t6.owner_id = t1.user_id AND t6.active IS TRUE
            WHERE
                t1.user_id >= 10000 AND
                t1.sign_in IS TRUE AND
                """ + query + """
                t3.roles && $""" + str(len(args) - 1) + """
            ORDER BY
                t4.time_sign DESC NULLS FIRST, t1.time_sign DESC
            OFFSET
                $""" + str(len(args)) + """
            LIMIT
                25""",
        *args
    )
    return (amount, [ dict(item) for item in data ])
