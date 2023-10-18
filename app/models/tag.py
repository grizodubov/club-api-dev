import re

from app.core.context import get_api_context



####################################################################
async def get_tags():
    api = get_api_context()
    result = {}
    data = await api.pg.club.fetch(
        """SELECT
                tags AS competency,
                interests AS interests,
                '' AS communities
            FROM
                users_tags
            UNION ALL
            SELECT
                '' AS competency,
                '' AS interests,
                tags AS communities
            FROM
                communities"""
    )
    ts = { 'competency', 'interests', 'communities' }
    for row in data:
        for t in ts:
            if row[t]:
                for k, v in parse_tags(row[t]):
                    if k in result:
                        result[k][t] += 1
                        result[k]['options'].update(v)
                    else:
                        result[k] = { kt: 1 if kt == t else 0 for kt in ts }
                        result[k]['options'] = v



####################################################################
def parse_tags(field):
    result = {}
    for part in re.split(r'\s*,\s*', field):
        k = lower(part)
        if k in result:
            result[k].add(part)
        else:
            result[k] = { part }
    return result
