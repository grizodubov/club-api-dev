from app.core.context import get_api_context



async def get_chats(user_id, chat_id = None):
    data = await api.pg.club.fetch(
        """SELECT
                t3.chat_id,
                t3.chat_model,
                coalesce(t5.name, t6.name) AS chat_name,
                t3.messages_unread,
                t3.message_id,
                t4.text AS message_text,
                t4.time_create AS message_time_create
            FROM
            (
            --
            SELECT
                t1.chat_id,
                (array_agg(chat_model))[1] AS chat_model,
                max(t1.id) AS message_id,
                sum(CASE WHEN t2.time_view IS NULL THEN 1 ELSE 0 END) AS messages_unread,
                bool_or(CASE WHEN t2.time_view IS NULL THEN TRUE ELSE FALSE END) AS messages_unread_exist
            FROM
            (
                SELECT
                    id, text, time_create, CASE WHEN target_id = $1 THEN author_id ELSE target_id END AS chat_id, 'user'::model AS chat_model
                FROM
                    messages
                WHERE
                    (target_id = $1 OR author_id = $1) AND
                    target_model = 'user'::model

                UNION ALL

                SELECT
                    id, text, time_create, target_id AS chat_id, 'group'::model AS chat_model
                FROM
                    messages
                WHERE
                    target_id IN (SELECT group_id FROM groups_users WHERE user_id = $1)
            ) t1
            LEFT JOIN
                items_views t2 ON t2.item_id = t1.id AND t2.user_id = $1
            GROUP BY t1.chat_id
            --
            ) t3
            LEFT JOIN
                messages t4 ON t4.id = t3.message_id
            LEFT JOIN
                users t5 ON t5.id = t3.chat_id
            LEFT JOIN
                groups t6 ON t6.id = t3.chat_id
            ORDER BY
                t3.messages_unread_exist DESC, t4.time_create DESC""",
        user_id
    )
    chats = [ dict(item) for item in data ]
    chats_ids = [ item['chat_id'] for item in chats ]
    if chat_id and chat_id not in chats_ids:
        name = await api.pg.club.fetchval( 
            """SELECT name FROM users WHERE id = $1""", chat_id
        )
        chats.append({
            'chat_id': chat_id,
            'chat_model': 'user',
            'chat_name': name,
            'messages_unread': 0,
            'message_id': None,
            'message_text': None,
            'message_time_create': None,
        })
    return {
        'chats': chats,
    }



async def get_messages(user_id, chat_id, chat_model, slice = None, before = False):
    query = ''
    args = []
    if slice:
        if before:


    if chat_model = 'group':
        data = await api.pg.club.fetch(
            """SELECT
                    t1.id, t1.time_create, t1.author_id, t3.name AS author_name, t1.text, t4.time_view
                FROM
                    messages t1
                INNER JOIN
                    groups_users t2 ON t2.group_id = t1.target_id
                INNER JOIN
                    users t3 ON t3.id = t1.author_id
                LEFT JOIN
                    items_views t4 ON t4.item_id = t1.id AND t4.user_id = $1
                WHERE
                    t1.target_id = $2 AND t2.user_id = $1
                ORDER BY
                    t1 DESC""",
            user_id, chat_id
        )
    else:
        data = await api.pg.club.fetch(
            """SELECT
                    t1.id, t1.time_create, t1.author_id, t3.name AS author_name, t1.text, t4.time_view
                FROM
                    messages t1
                INNER JOIN
                    users t3 ON t3.id = t1.author_id
                LEFT JOIN
                    items_views t4 ON t4.item_id = t1.id AND t4.user_id = $1
                WHERE
                    (t1.author_id = $1 AND t1.target_id = $2) OR
                    (t1.author_id = $2 AND t1.target_id = $1)
                ORDER BY
                    t1 DESC""",
            user_id, chat_id
        )
