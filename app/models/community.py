import re
import os.path
from functools import cmp_to_key

from app.core.context import get_api_context



####################################################################
class Community:


    ################################################################
    def __init__(self):
        self.id = 0
        self.time_create = None
        self.time_update = None
        self.name = ''
        self.description = ''
        self.avatar_hash = None
        self.members = []


    ################################################################
    @classmethod
    async def search(cls, text, offset = None, limit = None, count = False):
        api = get_api_context()
        result = []
        slice_query = ''
        conditions = [ 't1.id >= 10000' ]
        condition_query = ''
        args = []
        if text:
            conditions.append("""to_tsvector(concat_ws(' ', t1.name, t1.description)) @@ to_tsquery($1)""")
            args.append(re.sub(r'\s+', ' | ', text))
        if offset and limit:
            slice_query = ' OFFSET $2 LIMIT $3'
            args.extend([ offset, limit ])
        if conditions:
            conditions_query = ' WHERE ' + ' AND '.join(conditions)
        data = await api.pg.club.fetch(
            """SELECT
                    t1.id, t1.time_create, t1.time_update,
                    t1.name, t1.description, coalesce(t2.members, '{}'::bigint[]) AS members,
                    t8.hash AS avatar_hash
                FROM
                    communities t1
                LEFT JOIN
                    avatars t8 ON t8.owner_id = t1.id AND t8.active IS TRUE
                LEFT JOIN
                    (
                        SELECT
                            community_id, array_agg(user_id) AS members
                        FROM
                            communities_members
                        GROUP BY
                            community_id
                    ) t2 ON t2.community_id = t1.id""" + conditions_query + ' ORDER BY t1.name' + slice_query,
            *args
        )
        for row in data:
            item = Community()
            item.__dict__ = dict(row)
            result.append(item)
        if count:
            amount = len(result)
            if offset and limit:
                amount = await api.pg.club.fetchval(
                    """SELECT
                            count(t1.id)
                        FROM
                            communities t1""" + conditions_query,
                    *args
                )
            return (result, amount)
        return result
    
    
    ################################################################
    def reset(self):
        self.__init__()


    ################################################################
    def show(self):
        filter = { 'time_create', 'time_update' }
        return { k: v for k, v in self.__dict__.items() if not k.startswith('_') and k not in filter }


    ################################################################
    def dump(self):
        return { k: v for k, v in self.__dict__.items() }


    ################################################################
    async def set(self, id):
        api = get_api_context()
        if id:
            data = await api.pg.club.fetchrow(
                """SELECT
                        t1.id, t1.time_create, t1.time_update,
                        t1.name, t1.description, coalesce(t2.members, '{}'::bigint[]) AS members,
                        t8.hash AS avatar_hash
                    FROM
                        communities t1
                    LEFT JOIN
                        avatars t8 ON t8.owner_id = t1.id AND t8.active IS TRUE
                    LEFT JOIN
                        (
                            SELECT
                                community_id, array_agg(user_id) AS members
                            FROM
                                communities_members
                            GROUP BY
                                community_id
                        ) t2 ON t2.community_id = t1.id
                    WHERE
                        t1.id = $1""",
                id
            )
            self.__dict__ = dict(data)


    ################################################################
    async def update(self, **kwargs):
        api = get_api_context()
        cursor = 2
        query = []
        args = []
        for k in { 'name', 'description' }:
            if k in kwargs:
                query.append(k + ' = $' + str(cursor))
                args.append(kwargs[k])
                cursor += 1
        if query:
            await api.pg.club.execute(
                """UPDATE
                        communities
                    SET
                        """ + ', '.join(query) + """
                    WHERE
                        id = $1""",
                self.id, *args
            )
        if 'members' in kwargs:
            ids_to_delete = set(self.members) - set(kwargs['members'])
            if ids_to_delete:
                await api.pg.club.execute(
                    """DELETE FROM communities_members WHERE community_id = $1 AND user_id = ANY($2)""",
                    self.id, list(ids_to_delete)
                )
            ids_to_add = set(kwargs['members']) - set(self.members)
            if ids_to_add:
                cursor = 2
                query = []
                args = []
                for u in ids_to_add:
                    query.append('($1, $' + str(cursor) + ')')
                    args.append(u)
                    cursor += 1
                await api.pg.club.execute(
                    """INSERT INTO
                            communities_members (community_id, user_id)
                        VALUES """ + ', '.join(query),
                    self.id, *args
                )


    ################################################################
    def copy(self, user):
        self.__dict__ = user.__dict__.copy()


    ################################################################
    async def create(self, **kwargs):
        api = get_api_context()
        id = await api.pg.club.fetchval(
            """INSERT INTO
                    communities (name, description)
                VALUES
                    ($1, $2)
                RETURNING
                    id""",
            kwargs['name'],
            kwargs['description']
        )
        await self.set(id = id)



###############################################################
async def find_questions(community_id, words):
    api = get_api_context()
    data = await api.pg.club.fetch(
        """SELECT
                t1.id, t1.community_id, t1.text, ts_rank(t1.text_ts, to_tsquery('russian', $1)) AS rank,
                t2.name AS community_name, t1.time_create
            FROM
                posts t1
            INNER JOIN
                communities t2 ON t2.id = t1.community_id
            WHERE
                    t1.reply_to_post_id IS NULL
                AND
                    t1.community_id = $2
                AND
                    t1.text_ts @@ to_tsquery('russian', $1)
            ORDER BY
                ts_rank(t1.text_ts, to_tsquery('russian', $1)) DESC""",
        ' | '.join(words), community_id
    )
    return [ dict(item) for item in data ]



###############################################################
async def get_stats(communities_ids, user_id):
    if not communities_ids:
        return {}
    api = get_api_context()
    data = await api.pg.club.fetch(
        """SELECT
                t1.community_id,
                count(t1.id) FILTER (WHERE t1.reply_to_post_id IS NULL) AS subjects_open,
                count(t1.id) FILTER (WHERE t1.reply_to_post_id IS NULL AND t2.time_view IS NULL) AS subjects_new,
                count(t1.id) FILTER (WHERE t1.reply_to_post_id IS NOT NULL AND t2.time_view IS NULL) AS answers_new,
                max(t1.time_create) AS time_last_post,
                max(t1.time_create) FILTER (WHERE t2.time_view IS NULL) AS time_last_post_new
            FROM
                posts t1
            LEFT JOIN
                items_views t2 ON t2.item_id = t1.id AND t2.user_id = $2
            WHERE
                t1.community_id = ANY($1)
            GROUP BY
                t1.community_id""",
        communities_ids, user_id
    )
    return {
        str(row['community_id']): {
            'subjects_open': row['subjects_open'],
            'subjects_new': row['subjects_new'],
            'answers_new': row['answers_new'],
            'time_last_post': row['time_last_post'],
            'time_last_post_new': row['time_last_post_new'],
        }
        for row in data
    }



###############################################################
async def get_posts(community_id, user_id):
    if not community_id:
        return []
    api = get_api_context()
    data = await api.pg.club.fetch(
        """SELECT
                *
            FROM (
                SELECT
                    t1.id,
                    t1.time_create,
                    t1.time_update,
                    t1.community_id,
                    t1.text,
                    t1.reply_to_post_id,
                    t1.author_id,
                    t1.closed,
                    t1.helpful,
                    t2.name AS author_name,
                    t3.time_view,
                    coalesce(t1.reply_to_post_id, t1.id) AS question_id,
                    t8.hash AS author_avatar_hash
                FROM
                    posts t1
                INNER JOIN
                    users t2
                ON
                    t2.id = t1.author_id
                LEFT JOIN
                    items_views t3
                ON
                    t3.item_id = t1.id AND t3.user_id = $2
                LEFT JOIN
                    avatars t8
                ON
                    t8.owner_id = t2.id AND t8.active IS TRUE
                WHERE
                    t1.community_id = $1
            ) t4
            ORDER BY
                t4.question_id, t4.id""",
        community_id, user_id
    )
    unique_authors_ids = set([ item['author_id'] for item in data ])
    temp = {}
    for item in data:
        if item['reply_to_post_id'] is None:
            q = str(item['question_id'])
            if q not in temp:
                temp[q] = {
                    'question': dict(item),
                    'answers': [],
                    'time_last_post': item['time_create'],
                    'time_last_post_new': item['time_create'] if item['time_view'] is None else None
                }
    for item in data:
        if item['reply_to_post_id'] is not None:
            q = str(item['question_id'])
            if q in temp:
                temp[q]['answers'].append(dict(item))
                if temp[q]['time_last_post'] < item['time_create']:
                    temp[q]['time_last_post'] = item['time_create']
                if item['time_view'] is None:
                    if temp[q]['time_last_post_new'] is None or temp[q]['time_last_post_new'] < item['time_create']:
                        temp[q]['time_last_post_new'] = item['time_create']
    return [ v for v in temp.values() ]



###############################################################
async def add_post(community_id, user_id, text, reply_to_post_id = None):
    api = get_api_context()
    data = await api.pg.club.fetchrow( 
        """INSERT INTO
                posts
                (author_id, community_id, reply_to_post_id, text)
            VALUES
                ($1, $2, $3, $4)
            RETURNING
                id, time_create""",
        user_id, community_id, reply_to_post_id, text
    )
    await api.pg.club.execute( 
        """INSERT INTO
                items_views
                (item_id, user_id, time_view)
            VALUES
                ($1, $2, $3)
            ON CONFLICT
                (item_id, user_id)
            DO NOTHING""",
        data['id'], user_id, data['time_create']
    )
    return dict(data)



###############################################################
async def update_post(post_id, params):
    api = get_api_context()
    query = []
    args = []
    i = 2
    for k, v in params.items():
        if k in { 'closed', 'helpful' }:
            query.append(k + ' = $' + str(i))
            args.append(v)
            i += 1
    if query:
        await api.pg.club.fetchrow( 
            """UPDATE
                    posts
                SET """ + ', '.join(query) + """
                WHERE
                    id = $1""",
            post_id, *args
        )



###############################################################
async def check_post(community_id, reply_to_post_id):
    api = get_api_context()
    data = await api.pg.club.fetchval( 
        """SELECT id FROM posts WHERE community_id = $1 AND id = $2 AND reply_to_post_id IS NULL AND closed IS FALSE""",
        community_id, reply_to_post_id
    )
    if data:
        return True
    return False



###############################################################
async def check_question(post_id, user_id):
    api = get_api_context()
    community_id = await api.pg.club.fetchval( 
        """SELECT community_id FROM posts WHERE id = $1 AND author_id = $2 AND reply_to_post_id IS NULL""",
        post_id, user_id
    )
    if community_id:
        return community_id
    return None



###############################################################
async def check_answer(post_id, user_id, helpful = False):
    api = get_api_context()
    row = await api.pg.club.fetchrow( 
        """SELECT
                t1.community_id, t1.reply_to_post_id, t1.author_id
            FROM
                posts t1
            INNER JOIN
                posts t2 ON t2.id = t1.reply_to_post_id
            WHERE
                t1.id = $1 AND t2.author_id = $2""",
        post_id, user_id
    )
    if row:
        if helpful:
            count = await api.pg.club.fetchval(
                """SELECT
                        count(id)
                    FROM
                        posts
                    WHERE
                        author_id = $1 AND
                        reply_to_post_id = $2 AND
                        helpful IS TRUE""",
                row['author_id'], row['reply_to_post_id']
            )
            if count == 0:
                return row['community_id']
            else:
                return None
        return row['community_id']
    return None



###############################################################
def sort_communities(communities, stats):
    def communities_compare(a, b):
        id_a = str(a['id'])
        id_b = str(b['id'])
        weight_a = 0
        weight_b = 0
        if id_a in stats and id_b in stats:
            if stats[id_a]['subjects_open']:
                weight_a += 100
            if stats[id_b]['subjects_open']:
                weight_b += 100
            if stats[id_a]['subjects_new'] or stats[id_a]['answers_new']:
                weight_a += 10
            if stats[id_b]['subjects_new'] or stats[id_b]['answers_new']:
                weight_b += 10
            if stats[id_a]['time_last_post'] > stats[id_b]['time_last_post']:
                weight_a += 1
            elif stats[id_a]['time_last_post'] < stats[id_b]['time_last_post']:
                weight_b += 1
            if weight_a < weight_b:
                return 1
            if weight_a > weight_b:
                return -1
        return 0
    return sorted(communities, key = cmp_to_key(communities_compare))



###############################################################
async def extra_update_post(post_id, data):
    api = get_api_context()
    temp = await api.pg.club.fetchrow(
        """SELECT
                t1.id AS post_id, t1.author_id AS post_author_id, t1.closed AS post_closed, t1.helpful AS post_helpful,
                t2.id AS parent_id, t2.author_id AS parent_author_id, t2.closed AS parent_closed, t2.helpful AS parent_helpful
            FROM
                posts t1
            LEFT JOIN
                posts t2 ON t2.id = t1.reply_to_post_id
            WHERE
                t1.id = $1""",
        post_id
    )
    if not temp:
        return False
    if data['closed'] and temp['parent_id'] is not None:
        return False
    if data['helpful'] and (temp['parent_id'] is None or temp['post_author_id'] == temp['parent_author_id']):
        return False
    id = await api.pg.club.fetchval( 
        """UPDATE
                posts
            SET
                text = $2,
                helpful = $3,
                closed = $4
            WHERE
                id = $1
            RETURNING
                id""",
        post_id, data['text'], data['helpful'], data['closed']
    )
    if not id:
        return False
    return True



###############################################################
async def extra_delete_post(post_id):
    api = get_api_context()
    await api.pg.club.execute( 
        """DELETE FROM
                posts
            WHERE
                id = $1""",
        post_id
    )
    return True
