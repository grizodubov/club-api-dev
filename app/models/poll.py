import re
from datetime import datetime
import pytz

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
        self.wide = False
        self.rating = False
        self.rating_format = None
        self.many= False
        self.tags = ''
        self.score = 0
        self.votes = {}
        self.show_results = False


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
                    t1.id, t1.time_create, t1.time_update, t1.many, t1.show_results,
                    t1.community_id, t1.community_id_deleted, t4.name AS community_name,
                    t1.text, t1.answers, t1.active, t1.closed, t1.wide, t1.rating, t1.rating_format, t1.score,
                    t1.tags, coalesce(t2.votes, '{}'::jsonb)::jsonb || coalesce(t3.votes, '{}'::jsonb)::jsonb AS votes
                FROM
                    polls t1
                LEFT JOIN
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
        filter = { 'time_create', 'time_update', 'score' }
        return { k: v for k, v in self.__dict__.items() if not k.startswith('_') and k not in filter }


    ################################################################
    def show_with_score(self):
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
                    t1.id, t1.time_create, t1.time_update, t1.many, t1.show_results,
                    t1.community_id, t1.community_id_deleted, t4.name AS community_name,
                    t1.text, t1.answers, t1.active, t1.closed, t1.wide, t1.rating, t1.rating_format, t1.score,
                    t1.tags, coalesce(t2.votes, '{}'::jsonb)::jsonb || coalesce(t3.votes, '{}'::jsonb)::jsonb AS votes
                FROM
                    polls t1
                LEFT JOIN
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
        if not kwargs['rating'] and kwargs['tags'] and kwargs['tags'].strip():
            temp = ','.join([ t for t in re.split(r'\s*,\s*', kwargs['tags'].strip()) if t ])
        id = await api.pg.club.fetchval(
            """INSERT INTO
                    polls (community_id, text, answers, active, closed, tags, wide, rating, rating_format, many, score, show_results)
                VALUES
                    ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                RETURNING
                    id""",
            kwargs['community_id'] if not kwargs['rating'] else None,
            kwargs['text'],
            kwargs['answers'],
            kwargs['active'],
            kwargs['closed'],
            temp,
            kwargs['wide'],
            kwargs['rating'],
            kwargs['rating_format'] if kwargs['rating'] else None,
            kwargs['many'],
            kwargs['score'],
            kwargs['show_results'],
        )
        await self.set(id = id)



    ################################################################
    async def update(self, **kwargs):
        api = get_api_context()
        cursor = 2
        query = []
        args = []
        for k in { 'active', 'closed', 'text', 'tags', 'answers', 'community_id', 'wide', 'rating', 'rating_format', 'many', 'score', 'show_results' }:
            if k in kwargs:
                query.append(k + ' = $' + str(cursor))
                if k == 'tags':
                    temp = None
                    if not kwargs['rating'] and kwargs[k] and kwargs[k].strip():
                        temp = ','.join([ t for t in re.split(r'\s*,\s*', kwargs[k].strip()) if t ])
                elif k == 'community_id':
                    if not kwargs['rating']:
                        temp = kwargs[k]
                    else:
                        temp = None
                elif k == 'rating_format':
                    if kwargs['rating']:
                        temp = kwargs[k]
                    else:
                        temp = None
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
        votes = vote if type(vote) == list else [ vote ]
        args = [ self.id, user_id ]
        query = []
        for v in votes:
            args.append(v)
            query.append('($1, $2, $' + str(len(args)) + ')')
        await api.pg.club.execute( 
            """INSERT INTO
                    polls_votes (poll_id, user_id, answer)
                VALUES """ + ', '.join(query),
            *args
        )
        if self.score:
            await api.pg.club.execute( 
                """UPDATE users SET score = score + $1 WHERE id = $2""",
                self.score, user_id
            )


    ################################################################
    async def get_votes_log(self):
        api = get_api_context()
        data = await api.pg.club.fetch( 
            """SELECT
                    t1.user_id, t1.answer, GREATEST (t1.time_create, t1.time_update) AS time, t2.name
                FROM
                    polls_votes t1
                INNER JOIN
                    users t2 on t2.id = t1.user_id
                WHERE
                    t1.poll_id = $1
                ORDER BY
                    GREATEST (t1.time_create, t1.time_update)""",
            self.id
        )
        return [ dict(item) for item in data ]



###############################################################
async def get_user_polls_recommendations(user):
    result = []
    tags = ''
    if user.tags:
        tags += user.tags
    if user.interests:
        if tags:
            tags += ','
        tags += user.interests
    query = ' | '.join([ re.sub(r'\s+', ' & ', t.strip()) for t in tags.split(',') ])
    api = get_api_context()
    data = await api.pg.club.fetch(
        """SELECT
                    t1.id, t1.time_create, t1.time_update, t1.many, t1.show_results,
                    t1.community_id, t1.community_id_deleted, t4.name AS community_name,
                    t1.text, t1.answers, t1.active, t1.closed, t1.wide, t1.rating, t1.rating_format, t1.score,
                    t1.tags, coalesce(t2.votes, '{}'::jsonb)::jsonb || coalesce(t3.votes, '{}'::jsonb)::jsonb AS votes
                FROM
                    polls t1
                LEFT JOIN
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
                LEFT JOIN
                    polls_votes t5 ON t5.poll_id = t1.id AND t5.user_id = $1    
                WHERE
                    t1.active IS TRUE AND
                    t1.closed IS FALSE AND
                    t1.rating IS FALSE AND
                    t5.answer IS NULL AND
                    (t1.wide IS TRUE OR to_tsvector(t1.tags) @@ to_tsquery($2))
                ORDER BY t1.id DESC""",
        user.id, query
    )
    for row in data:
        item = Poll()
        item.__dict__ = dict(row)
        result.append(item)
    return result



###############################################################
async def get_user_rating_polls(user):
    polls = []
    votes = []
    api = get_api_context()

    # Раз в месяц
    dt = datetime.fromtimestamp(round(user.time_create / 1000), pytz.utc)
    dt_now = datetime.now(tz = pytz.utc)
    if dt_now.year > dt.year or dt_now.month > dt.month:
        day = 5
        if dt.day >= 6 and dt.day <= 10:
            day = 10
        elif dt.day >= 11 and dt.day <= 15:
            day = 15
        elif dt.day >= 16 and dt.day <= 20:
            day = 20
        elif dt.day >= 21:
            day = 25
        dt_control1 = datetime(dt_now.year, dt_now.month, 1, tzinfo = pytz.utc)
        if dt_now.month == 1:
            dt_control2_1 = datetime(dt_now.year - 1, 12, 1, tzinfo = pytz.utc)
        else:
            dt_control2_1 = datetime(dt_now.year, dt_now.month - 1, 1, tzinfo = pytz.utc)
        dt_control2_2 = datetime(dt_now.year, dt_now.month, day, tzinfo = pytz.utc)

        # datetime(2010, 9, 12
        # datetime.datetime.now(tz = pytz.utc)
        # print(dt.strftime('%Y-%m-%d %H:%M:%S'))
        # a = '2010-01-31'
        # datee = datetime.datetime.strptime(a, "%Y-%m-%d")

        data = await api.pg.club.fetch(
            """SELECT
                    t1.id, t1.time_create, t1.time_update, t1.text, t1.answers
                FROM
                    polls t1
                WHERE
                    t1.active IS TRUE AND
                    t1.closed IS FALSE AND
                    t1.rating IS TRUE AND
                    (
                        (
                            t1.rating_format = 'Один раз' AND
                            t1.id NOT IN (
                                SELECT
                                    s1.poll_id
                                FROM
                                    polls_votes s1
                                WHERE
                                    s1.user_id = $1
                            )
                        ) OR
                        (
                            t1.rating_format = 'Каждый месяц' AND
                            t1.id NOT IN (
                                SELECT
                                    s2.poll_id
                                FROM
                                    polls_votes s2
                                WHERE
                                    s2.user_id = $1 AND
                                    (
                                        s2.time_create >= $2 OR
                                        (
                                            s2.time_create >= $3 AND
                                            now() at time zone 'utc' < $4
                                        )
                                    )
                            )
                        )
                    )
                ORDER BY t1.id DESC""",
            user.id, dt_control1.timestamp() * 1000, dt_control2_1.timestamp() * 1000, dt_control2_2.timestamp() * 1000
        )
        cache = {}
        if data:
            for item in data:
                cache[str(item['id'])] = True
                polls.append(dict(item))
        data = await api.pg.club.fetch(
            """SELECT
                    v2.id, v2.text, v2.answers[v1.answer] AS answer
                FROM
                    polls_votes v1
                INNER JOIN
                    polls v2 ON v2.id = v1.poll_id
                WHERE
                    v1.user_id = $1 AND
                    v2.active IS TRUE AND
                    v2.rating IS TRUE AND
                    v2.closed IS FALSE AND
                    (
                        (
                            v2.rating_format = 'Один раз' AND
                            v1.time_create >= $2
                        ) OR
                        (
                            v2.rating_format = 'Каждый месяц' AND
                            v1.time_create >= $2
                        )
                    )
                ORDER BY
                    v1.time_create DESC""",

            user.id, dt_control2_1.timestamp() * 1000
        )
        if data:
            for item in data:
                if str(item['id']) not in cache:
                    cache[str(item['id'])] = True
                    votes.append(dict(item))
    return {
        'polls': polls,
        'votes': votes,
    }
