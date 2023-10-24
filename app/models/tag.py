import re
#import pprint

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
                communities
            WHERE tags IS NOT NULL
            UNION ALL
            SELECT
                '' AS competency,
                '' AS interests,
                tags AS communities
            FROM
                posts
            WHERE reply_to_post_id IS NULL AND tags IS NOT NULL"""
    )
    ts = { 'competency', 'interests', 'communities' }
    for row in data:
        for t in ts:
            if row[t]:
                temp = parse_tags(row[t])
                for k, v in temp.items():
                    if k in result:
                        result[k][t] += 1
                        result[k]['options'].update(v)
                    else:
                        result[k] = { kt: 1 if kt == t else 0 for kt in ts }
                        result[k]['options'] = v
    return result



####################################################################
def parse_tags(field):
    result = {}
    for part in re.split(r'\s*,\s*', field):
        k = part.lower()
        if k in result:
            result[k].add(part)
        else:
            result[k] = { part }
    return result



####################################################################
async def update_tag(tag_from, tag_to):
    api = get_api_context()
    await api.pg.club.execute(
        """UPDATE
                users_tags
            SET
                tags = tag_replace(tags, $1, $2),
                interests = tag_replace(interests, $1, $2)""",
        tag_from, tag_to
    )
    await api.pg.club.execute(
        """UPDATE
                communities
            SET
                tags = tag_replace(tags, $1, $2)
            WHERE
                tags IS NOT NULL""",
        tag_from, tag_to
    )
    await api.pg.club.execute(
        """UPDATE
                posts
            SET
                tags = tag_replace(tags, $1, $2)
            WHERE
                reply_to_post_id IS NULL AND tags IS NOT NULL""",
        tag_from, tag_to
    )



# CREATE OR REPLACE FUNCTION tag_replace(s text, t text, r text) RETURNS text
# LANGUAGE plpgsql
# AS $$
#     DECLARE
#         data text[];
#         t_lower text := lower(t);
#         r_lower text := lower(r);
#         result text := '';
#         ft boolean := FALSE;
#         fr boolean := FALSE;
#         item text;
#         item_lower text;
#     BEGIN
#         data = regexp_split_to_array(s, '\s*,\s*');
#         IF data IS NOT NULL AND array_length(data, 1) > 0 THEN
#             FOREACH
#                 item IN ARRAY regexp_split_to_array(s, '\s*,\s*')
#             LOOP
#                 item_lower = lower(item);
#                 IF t_lower = item_lower OR r_lower = item_lower THEN
#                     IF r_lower = item_lower AND fr IS FALSE THEN
#                         fr := TRUE;
#                         IF ft IS NOT TRUE THEN
#                             IF result <> '' THEN result := result || ', '; END IF;
#                             result := result || r;
#                         END IF;
#                     END IF;
#                     IF t_lower = item_lower AND ft IS FALSE THEN
#                         ft := TRUE;
#                         IF fr IS NOT TRUE AND r <> '' THEN
#                             IF result <> '' THEN result := result || ', '; END IF;
#                             result := result || r;
#                         END IF;
#                     END IF;
#                 ELSE
#                     IF result <> '' THEN result := result || ', '; END IF;
#                     result := result || item;
#                 END IF;
#             END LOOP;
#             IF result = '' THEN
#                 RETURN NULL;
#             ELSE
#                 RETURN result;
#             END IF;
#         END IF;
#         RETURN NULL;
#     END;
# $$;
