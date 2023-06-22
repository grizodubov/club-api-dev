import os.path

from app.core.context import get_api_context



####################################################################
class Message:


    ################################################################
    def __init__(self):
        self.id = 0
        self.time_create = None
        self.time_update = None
        self.author_id = None
        self.author_id_deleted = None
        self.author_name = ''
        self.target_id = None
        self.target_id_deleted = None
        self.target_model = None
        self.target_name = ''
        self.reply_to_message_id = None
        self.reply_to_message_id_deleted = None
        self.text = ''
        self.time_view = None

    
    ################################################################
    def reset(self):
        self.__init__()


    ################################################################
    def show(self):
        return { k: v for k, v in self.__dict__.items() if not k.startswith('_') }


    ################################################################
    async def set(self, id):
        api = get_api_context()
        if id:
            data = await api.pg.club.fetchrow(
                """SELECT
                        t1.id, t1.time_create, t1.time_update,
                        t1.author_id, t1.author_id_deleted, t2.name AS author_name,
                        t1.target_id, t1.target_id_deleted, t1.target_model,
                        t1.reply_to_message_id, t1.reply_to_message_id_deleted,
                        t1.text, t3.time_view
                    FROM
                        messages t1
                    INNER JOIN
                        users t2 ON t2.id = t1.author_id OR t2.id = t1.author_id_deleted
                    LEFT JOIN
                        items_views t3 ON t3.item_id = t1.id
                    WHERE
                        t1.id = $1""",
                id
            )
            self.__dict__ = dict(data)
            if self.target_model == 'group' or self.target_model == 'user':
                self.target_name = await api.pg.club.fetchval(
                    """SELECT name FROM """ + self.target_model + """s WHERE id = $1""",
                    self.target_id
                )



async def get_chats(user_id, chat_id = None):
    api = get_api_context()
    data = await api.pg.club.fetch(
        """SELECT
                t3.chat_id,
                t3.chat_model,
                coalesce(t5.name, t6.name) AS chat_name,
                t5_1.status AS chat_status,
                t3.messages_unread,
                t3.min_message_id,
                t3.max_message_id,
                t4.text AS max_message_text,
                t4.time_create AS max_message_time_create
            FROM
            (
            --
                SELECT
                    t1.chat_id,
                    (array_agg(chat_model))[1] AS chat_model,
                    min(t1.id) AS min_message_id,
                    max(t1.id) AS max_message_id,
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

                UNION ALL

                SELECT
                    1050 AS chat_id, 'user' AS chat_model,
                    NULL AS min_message_id, NULL AS max_message_id,
                    0 AS messages_unread, FALSE AS messages_unread_exist
                WHERE NOT EXISTS (
                    SELECT id FROM messages WHERE (target_id = $1 AND author_id = 1050) OR (target_id = 1050 AND author_id = $1)
                )
            --
            ) t3
            LEFT JOIN
                messages t4 ON t4.id = t3.max_message_id
            LEFT JOIN
                users t5 ON t5.id = t3.chat_id
            LEFT JOIN
                users_info t5_1 ON t5_1.user_id = t5.id
            LEFT JOIN
                groups t6 ON t6.id = t3.chat_id
            ORDER BY
                t3.messages_unread_exist DESC, t4.time_create DESC, t3.chat_id""",
        user_id
    )
    chats = [ dict(item) | { 'avatar': check_avatar_by_id(item['chat_id']) } for item in data ]
    chats_ids = [ item['chat_id'] for item in chats ]
    if chat_id and chat_id not in chats_ids:
        name = await api.pg.club.fetchval( 
            """SELECT name FROM users WHERE id = $1""", chat_id
        )
        if name:
            chats.append({
                'chat_id': chat_id,
                'chat_model': 'user',
                'chat_name': name,
                'messages_unread': 0,
                'min_message_id': None,
                'max_message_id': None,
                'max_message_text': None,
                'max_message_time_create': None,
                'avatar': check_avatar_by_id(chat_id),
            })
        else:
            name = await api.pg.club.fetchval( 
                """SELECT name FROM groups WHERE id = $1""", chat_id
            )
            if name:
                chats.append({
                    'chat_id': chat_id,
                    'chat_model': 'group',
                    'chat_name': name,
                    'messages_unread': 0,
                    'min_message_id': None,
                    'max_message_id': None,
                    'max_message_text': None,
                    'max_message_time_create': None,
                    'avatar': check_avatar_by_id(chat_id),
                })
    return chats



async def get_messages(user_id, chat_id, chat_model, init = False, vector = None):
    api = get_api_context()
    fragments = []
    if init:
        last_read_message_id = await api.pg.club.fetchval(
            """SELECT
                    max(t1.id)
                FROM
                    messages t1
                INNER JOIN
                    items_views t2 ON t2.item_id = t1.id
                WHERE
                    t2.user_id = $1""",
            user_id
        )
        if last_read_message_id:
            fragments = [
                { 'reverse': True, 'id': last_read_message_id, 'include': True },
                { 'reverse': False, 'id': last_read_message_id, 'include': False },
            ]
        else:
            fragments = [
                { 'reverse': False, 'id': None, 'include': False },
            ]
    elif vector:
        fragments = [
            { 'reverse': vector['reverse'], 'id': vector['id'], 'include': False },
        ]
    if fragments:
        return await query_messages(user_id, chat_id, chat_model, fragments)
    return []



async def query_messages(user_id, chat_id, chat_model, fragments):
    # fragment { reverse: True | False, id: [bigint], include: True | False }
    api = get_api_context()
    args = [ user_id, chat_id ]
    queries = []
    for fragment in fragments:
        if chat_model == 'group':
            query1 = """
                INNER JOIN
                    groups_users t2 ON t2.group_id = t1.target_id"""
            query2 = """
                    (t1.target_id = $2 AND t2.user_id = $1)"""
        else:
            query1 = ''
            query2 = """
                    ((t1.author_id = $1 AND t1.target_id = $2) OR
                    (t1.author_id = $2 AND t1.target_id = $1))"""
        if fragment['id']:
            if fragment['reverse']:
                op = '<=' if fragment['include'] else '<'
            else:
                op = '>=' if fragment['include'] else '>'
            query2 += ' AND t1.id ' + op + ' $' + str(len(args) + 1)
            args.append(fragment['id'])
        query = """
            (SELECT
                t1.id, t1.time_create, t1.author_id, t3.name AS author_name, t1.text, t4.time_view
            FROM
                messages t1 """ + query1 + """
            INNER JOIN
                users t3 ON t3.id = t1.author_id
            LEFT JOIN
                items_views t4 ON t4.item_id = t1.id AND t4.user_id = $1
            WHERE """ + query2 + """
            ORDER BY t1"""
        if fragment['reverse']:
            query += ' DESC'
        query += ' LIMIT 30)'
        queries.append(query)
    if len(queries) == 1:
        #print(queries[0])
        data = await api.pg.club.fetch(
            queries[0],
            *args
        )
    else:
        #print("""SELECT t.* FROM (""" + ' UNION ALL '.join(queries) + """) t ORDER BY t.id""")
        data = await api.pg.club.fetch(
            """SELECT t.* FROM (""" + ' UNION ALL '.join(queries) + """) t ORDER BY t.id""",
            *args
        )
    unique_authors_ids = set([ item['author_id'] for item in data ])
    authors_avatars = {}
    for author_id in unique_authors_ids:
        authors_avatars[str(author_id)] = check_avatar_by_id(author_id)
    return [ dict(item) | { 'author_avatar': authors_avatars[str(item['author_id'])] } for item in data ]



async def add_message(user_id, chat_id, chat_model, text):
    api = get_api_context()
    data = await api.pg.club.fetchrow( 
        """INSERT INTO
                messages
                (author_id, target_id, target_model, text)
            VALUES
                ($1, $2, $3, $4)
            RETURNING
                id, time_create, author_id, text""",
        user_id, chat_id, chat_model, text
    )
    message = dict(data)
    await api.pg.club.execute( 
        """INSERT INTO
                items_views
                (item_id, user_id, time_view)
            VALUES
                ($1, $2, $3)
            ON CONFLICT
                (item_id, user_id)
            DO NOTHING""",
        message['id'], user_id, message['time_create']
    )
    message['time_view'] = message['time_create']
    return message | { 'author_avatar': check_avatar_by_id(user_id) }



async def view_message(user_id, message_id):
    api = get_api_context()
    time_view = await api.pg.club.fetchval( 
        """INSERT INTO
                items_views
                (item_id, user_id)
            VALUES
                ($2, $1)
            ON CONFLICT
                (item_id, user_id)
            DO NOTHING
            RETURNING
                time_view""",
        user_id, message_id
    )
    return time_view



async def view_messages(user_id, messages_ids):
    api = get_api_context()
    query = []
    args = []
    for i, id in enumerate(messages_ids):
        query.append('($' + str(i + 2) + ', $1)')
        args.append(id)
    data = await api.pg.club.fetch( 
        """INSERT INTO
                items_views
                (item_id, user_id)
            VALUES """ + ', '.join(query) + """
            ON CONFLICT
                (item_id, user_id)
            DO NOTHING
            RETURNING
                item_id, time_view""",
        user_id, *args
    )
    return [ { 'message_id': row['item_id'], 'time_view': row['time_view'] } for row in data ]



################################################################
def check_avatar_by_id(id):
    return os.path.isfile('/var/www/media.clubgermes.ru/html/avatars/' + str(id) + '.jpg')



################################################################
async def check_recepient(user_id, recepient_id):
    api = get_api_context()
    amount = await api.pg.club.fetchval( 
        """SELECT
                count(id)
            FROM
                messages
            WHERE
                target_id = $1 AND author_id = $2""",
        user_id, recepient_id
    )
    return True if amount else False



################################################################
async def check_recepients(user_id, recepients_ids):
    api = get_api_context()
    data = await api.pg.club.fetch( 
        """SELECT
                author_id, count(id) AS amount
            FROM
                messages
            WHERE
                target_id = $1 AND author_id = ANY($2)
            GROUP BY
                author_id""",
        user_id, recepients_ids
    )
    return {
        str(row['author_id']): True if row['amount'] else False for row in data
    }
