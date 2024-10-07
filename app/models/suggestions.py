import asyncio
import math

from app.core.context import get_api_context



################################################################
async def suggestions_deactivate(api, user_id, suggestions):
    await api.pg.club.fetch(
        """UPDATE
                users_suggestions_log
            SET
                active = FALSE
            WHERE
                user_1_id = $1 OR user_2_id = $1""",
        user_id
    )



################################################################
async def suggestions_process(api, user_id, suggestions):
    filter = [ 'company scope', 'company needs', 'personal expertise', 'personal needs' ]
    mapping = {
        'company scope': 'company needs',
        'company needs': 'company scope',
        'personal expertise': 'personal needs',
        'personal needs': 'personal expertise',
    }
    data = await api.pg.club.fetch(
        """SELECT
                user_1_id, user_2_id, active, tags, tags_update
            FROM
                users_suggestions_log
            WHERE
                user_1_id = $1 OR user_2_id = $1""",
        user_id
    )
    suggestions_state = {}
    for item in suggestions:
        suggestions_state[str(item['id'])] = item
    state = {}
    for item in data:
        k = str(item['user_1_id']) if item['user_1_id'] != user_id else str(item['user_2_id'])
        state[k] = dict(item) | { 'id': int(k) }
    queries = []
    # process
    for k in suggestions_state.keys():
        if k not in state:
            # новая запись в совпадениях
            args = []
            if user_id < suggestions_state[k]['id']:
                args.extend([ user_id, suggestions_state[k]['id'] ])
            else:
                args.extend([ suggestions_state[k]['id'], user_id ])
            temp = {}
            for f in filter:
                if f + ' intersections' in suggestions_state[k]['tags']:
                    dk = ''
                    if user_id < suggestions_state[k]['id']:
                        dk = mapping[f] + ' : ' + f
                    else:
                        dk = f + ' : ' + mapping[f]
                    temp[dk] = suggestions_state[k]['tags'][f + ' intersections']
            if not temp:
                temp = None
            args.append(temp)
            queries.append(
                [
                    """INSERT INTO users_suggestions_log (user_1_id, user_2_id, tags) VALUES ($1, $2, $3) ON CONFLICT (user_1_id, user_2_id) DO NOTHING""",
                    args
                ]
            )
        else:
            # запись уже есть
            args = []
            if user_id < suggestions_state[k]['id']:
                args.extend([ user_id, suggestions_state[k]['id'] ])
            else:
                args.extend([ suggestions_state[k]['id'], user_id ])
            fields = []
            if state[k]['active'] is False:
                fields.extend([
                    'active = TRUE',
                    "time_refresh = now() at time zone 'utc'"
                ])
            temp = {}
            for f in filter:
                if f + ' intersections' in suggestions_state[k]['tags']:
                    dk = ''
                    if user_id < suggestions_state[k]['id']:
                        dk = mapping[f] + ' : ' + f
                    else:
                        dk = f + ' : ' + mapping[f]
                    temp[dk] = suggestions_state[k]['tags'][f + ' intersections']
            if not temp:
                temp = None
            d = {
                'add': {},
                'del': {},
            }
            if temp is not None:
                if state[k]['tags'] is None:
                    d['add'] = temp
                else:
                    for kc in temp.keys():
                        if kc not in state[k]['tags']:
                            d['add'][kc] = temp[kc]
                        else:
                            c1 = set(temp[kc])
                            c2 = set(state[k]['tags'][kc])
                            d_add = c1 - c2
                            d_del = c2 - c1
                        if d_add:
                            d['add'][kc] = list(d_add)
                        if d_del:
                            d['del'][kc] = list(d_del)
                    for kc in state[k]['tags'].keys():
                        if kc not in temp:
                            d['del'][kc] = state[k]['tags'][kc]
            else:
                if state[k]['tags'] is not None:
                    d['del'] = state[k]['tags']
            if d['add'] or d['del']:
                upd = {}
                if d['add']:
                    upd['add'] = d['add']
                if d['del']:
                    upd['del'] = d['del']
                fields.extend([
                    'tags = $3',
                    'tags_update = $4',
                    "time_update = now() at time zone 'utc'"
                ])
                args.extend([ temp, upd ])
            if fields:
                queries.append(
                    [
                        """UPDATE users_suggestions_log SET """ + ', '.join(fields) + """ WHERE user_1_id = $1 AND user_2_id = $2""",
                        args
                    ]
                )
    # remove tags
    for k in state.keys():
        if k not in suggestions_state and state[k]['tags'] is not None:
            args = []
            if user_id < state[k]['id']:
                args.extend([ user_id, state[k]['id'] ])
            else:
                args.extend([ state[k]['id'], user_id ])
            args.append({ 'add': {}, 'del': state[k]['tags'] })
            queries.append(
                [
                    """UPDATE users_suggestions_log SET tags = NULL, tags_update = $3, time_update = now() at time zone 'utc' WHERE user_1_id = $1 AND user_2_id = $2 AND active IS TRUE""",
                    args
                ]
            )
    # db update
    if queries:
        await asyncio.gather(*[
            api.pg.club.execute(q[0], *q[1]) for q in queries
        ])



################################################################
async def get_suggestions_list(page = 1, community_manager_id = None, evaluation = [ True, True, True ], date_evaluation = None):
    api = get_api_context()
    where = [ 'active IS TRUE', 'tags IS NOT NULL' ]
    args = []
    if evaluation[0] == False or evaluation[1] == False or evaluation[2] == False:
        if evaluation[0] and evaluation[1]:
            where.append('priority_1 IS FALSE')
        elif evaluation[0] and evaluation[2]:
            where.append('(priority_1 IS TRUE OR priority_2 IS FALSE)')
        elif evaluation[1] and evaluation[2]:
            where.append('(priority_1 IS TRUE OR priority_2 IS TRUE)')
        else:
            if evaluation[0]:
                where.extend([ 'priority_1 IS FALSE', 'priority_2 IS FALSE' ])
            elif evaluation[1]:
                where.extend([ 'priority_1 IS FALSE', 'priority_2 IS TRUE' ])
            elif evaluation[2]:
                where.append('priority_1 IS TRUE')
            else:
                where.append('FALSE')
    if community_manager_id and community_manager_id > 0:
        args.append(community_manager_id)
        where.append('(community_manager_1_id = ${v} OR community_manager_2_id = ${v})'.format(v = len(args)))
        if date_evaluation:
            args.append(date_evaluation)
            where.append("""(
                (community_manager_1_id = ${v} AND community_manager_1_time_max >= ((now() at time zone 'utc')::date - make_interval(days => ${t}))) OR 
                (community_manager_2_id = ${v} AND community_manager_2_time_max >= ((now() at time zone 'utc')::date - make_interval(days => ${t})))
            )""".format(v = len(args) - 1, t = len(args)))
    elif community_manager_id == 0:
        where.append('(community_manager_1_id IS NULL OR community_manager_2_id IS NULL)')
        if date_evaluation:
            args.append(date_evaluation)
            where.append("""community_managers_time_max >= ((now() at time zone 'utc')::date - make_interval(days => ${t}))""".format(t = len(args)))
    else:
        if date_evaluation:
            args.append(date_evaluation)
            where.append("""community_managers_time_max >= ((now() at time zone 'utc')::date - make_interval(days => ${t}))""".format(t = len(args)))
    amount = await api.pg.club.fetchrow(
        """SELECT
                count(*) AS all
            FROM
                users_suggestions_log_view
            WHERE """ + ' AND '.join(where),
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
        data = await api.pg.club.fetch(
            """SELECT
                    *
                FROM
                    users_suggestions_log_view
                WHERE """ + ' AND '.join(where) + """
                ORDER BY
                    priority_1 DESC, priority_2 DESC, time_max DESC
                OFFSET
                    ${v}
                LIMIT
                    25""".format(v = len(args)),
            *args
        )
        if data:
            all = [ dict(item) for item in data ]
    return (amount['all'], all, page)



################################################################
async def get_suggestions_comments(ids):
    api = get_api_context()
    result = {}
    subqueries = []
    args = []
    for item in ids:
        subqueries.append('(t1.user_id = ${v1} AND t1.partner_id = ${v2})'.format(v1 = len(args) + 1, v2 = len(args) + 2))
        subqueries.append('(t1.user_id = ${v1} AND t1.partner_id = ${v2})'.format(v1 = len(args) + 2, v2 = len(args) + 1))
        args.extend([ item[0], item[1] ])
    if subqueries:
        data = await api.pg.club.fetch(
            """SELECT
                    ct.user_id, ct.partner_id, array_agg(comment) AS comments
                FROM (
                    SELECT
                        t1.user_id, t1.partner_id, jsonb_build_object(
                            'id', t1.id,
                            'time_create', t1.time_create,
                            'comment', t1.comment,
                            'author_id', t1.author_id,
                            'author_name', t2.name
                        ) AS comment
                    FROM
                        suggestions_comments t1
                    INNER JOIN
                        users t2 ON t2.id = t1.author_id
                    WHERE """ + ' OR '.join(subqueries) + """
                    ORDER BY t1.id DESC
                ) ct
                GROUP BY
                    ct.user_id, ct.partner_id""",
            *args
        )
        if data:
            for item in data:
                result[str(item['user_id']) + '-' + str(item['partner_id'])] = item['comments']
    return result
