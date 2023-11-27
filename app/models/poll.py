import re

from app.core.context import get_api_context



####################################################################
class Poll:


    ################################################################
    def __init__(self):
        self.id = 0
        self.time_create = None
        self.time_update = None
        self.community_id = None
        self.community_name = ''
        self.community_id_deleted = None
        self.text = ''
        self.answers = []
        self.active = False
        self.closed = False
        self.tags = ''
        self.votes = {}


    ################################################################
    @classmethod
    async def search(cls, communities_ids = None, active = None, closed = None):
        api = get_api_context()
        result = []
        query = ''
        conditions = []
        args = []
        i = 0
        if communities_ids is not None:
            i += 1
            conditions.append('t1.community_id = ANY($' + str(i) + ')')
            args.append(communities_ids)
        if active is not None:
            if active:
                conditions.append('t1.active IS TRUE')
            else:
                conditions.append('t1.active IS FALSE')
        if closed is not None:
            if closed:
                conditions.append('t1.closed IS TRUE')
            else:
                conditions.append('t1.closed IS FALSE')
        if conditions:
            query = ' WHERE ' + ' AND '.join(conditions)
        data = await api.pg.club.fetch(
            """SELECT
                    t1.id, t1.time_create, t1.time_update,
                    t1.community_id, t1.community_id_deleted, t4.name AS community_name,
                    t1.text, t1.answers, t1.active, t1.closed,
                    t1.tags, coalesce(t2.votes, '{}'::jsonb)::jsonb || coalesce(t3.votes, '{}'::jsonb)::jsonb AS votes
                FROM
                    polls t1
                INNER JOIN
                    communities t4 ON t4.id = t1.community_id
                LEFT JOIN
                    (
                        SELECT
                            id, votes
                        FROM
                            polls, LATERAL (
                                SELECT
                                    jsonb_object_agg(n::text, '{}'::integer[]) AS votes
                                FROM
                                    unnest(array_positions(array_fill(1, ARRAY[coalesce(array_length(answers, 1), 0)]), 1)) AS n
                            ) sub1
                    ) t2 ON t2.id = t1.id
                LEFT JOIN
                    (
                        SELECT
                            poll_id, jsonb_object_agg(answer, votes) AS votes
                        FROM
                            (
                                SELECT
                                    poll_id, answer, array_agg(user_id) AS votes
                                FROM
                                    polls_votes
                                GROUP BY
                                    poll_id, answer
                            ) sub2
                        GROUP BY
                            poll_id
                    ) t3 ON t3.poll_id = t1.id""" + query + """ ORDER BY t1.id DESC""",
            *args
        )
        for row in data:
            item = Poll()
            item.__dict__ = dict(row)
            result.append(item)
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
                    t1.community_id, t1.community_id_deleted, t4.name AS community_name,
                    t1.text, t1.answers, t1.active, t1.closed,
                    t1.tags, coalesce(t2.votes, '{}'::jsonb)::jsonb || coalesce(t3.votes, '{}'::jsonb)::jsonb AS votes
                FROM
                    polls t1
                INNER JOIN
                    communities t4 ON t4.id = t1.community_id
                LEFT JOIN
                    (
                        SELECT
                            id, votes
                        FROM
                            polls, LATERAL (
                                SELECT
                                    jsonb_object_agg(n::text, '{}'::integer[]) AS votes
                                FROM
                                    unnest(array_positions(array_fill(1, ARRAY[coalesce(array_length(answers, 1), 0)]), 1)) AS n
                            ) sub1
                    ) t2 ON t2.id = t1.id
                LEFT JOIN
                    (
                        SELECT
                            poll_id, jsonb_object_agg(answer, votes) AS votes
                        FROM
                            (
                                SELECT
                                    poll_id, answer, array_agg(user_id) AS votes
                                FROM
                                    polls_votes
                                GROUP BY
                                    poll_id, answer
                            ) sub2
                        GROUP BY
                            poll_id
                    ) t3 ON t3.poll_id = t1.id
                WHERE
                    t1.id = $1""",
                id
            )
            self.__dict__ = dict(data)


    ################################################################
    async def create(self, **kwargs):
        api = get_api_context()
        temp = None
        if kwargs['tags'] and kwargs['tags'].strip():
            temp = ','.join([ t for t in re.split(r'\s*,\s*', kwargs['tags'].strip()) if t ])
        id = await api.pg.club.fetchval(
            """INSERT INTO
                    polls (community_id, text, answers, active, closed, tags)
                VALUES
                    ($1, $2, $3, $4, $5, $6)
                RETURNING
                    id""",
            kwargs['community_id'],
            kwargs['text'],
            kwargs['answers'],
            kwargs['active'],
            kwargs['closed'],
            temp,
        )
        await self.set(id = id)



    ################################################################
    async def update(self, **kwargs):
        api = get_api_context()
        cursor = 2
        query = []
        args = []
        for k in { 'active', 'closed', 'text', 'tags', 'answers', 'community_id' }:
            if k in kwargs:
                query.append(k + ' = $' + str(cursor))
                if k == 'tags':
                    temp = None
                    if kwargs[k] and kwargs[k].strip():
                        temp = ','.join([ t for t in re.split(r'\s*,\s*', kwargs[k].strip()) if t ])
                else:
                    temp = kwargs[k]
                args.append(temp)
                cursor += 1
        if 'history' in kwargs and kwargs['history']:
            for step in kwargs['history']:
                if step.startswith('d'):
                    num = None
                    try:
                        num = int(step[1:])
                    except:
                        pass
                    # print('NUM', num)
                    if num:
                        await api.pg.club.execute(
                            """DELETE FROM polls_votes WHERE poll_id = $1 AND answer = $2""",
                            self.id, num
                        )
                        await api.pg.club.execute(
                            """UPDATE polls_votes SET answer = answer - 1 WHERE poll_id = $1 AND answer > $2""",
                            self.id, num
                        )
        if query:
            # print(query)
            await api.pg.club.execute(
                """UPDATE
                        polls
                    SET
                        """ + ', '.join(query) + """
                    WHERE
                        id = $1""",
                self.id, *args
            )



    ################################################################
    async def add_vote(self, user_id, vote):
        api = get_api_context()
        await api.pg.club.execute( 
            """INSERT INTO
                    polls_votes (poll_id, user_id, answer)
                VALUES ($1, $2, $3)
                ON CONFLICT (poll_id, user_id)
                DO UPDATE SET
                    answer = EXCLUDED.answer""",
            self.id, user_id, vote
        )
