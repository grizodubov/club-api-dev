import re
import asyncio
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



####################################################################
def parse_tags_1(field):
    result = {}
    for part in re.split(r'\s*\+\s*', field):
        if part:
            k = part.lower()
            if k in result:
                result[k].add(part)
            else:
                result[k] = { part }
    return result



####################################################################
async def tag_1_get_categories():
    api = get_api_context()
    data = await api.pg.club.fetch(
        """SELECT
                t1.alias, t1.name, t1.catalog, t1.allow_user_tags
            FROM
                tags_categories t1
            JOIN
                unnest('{"company scope", "company needs", "personal expertise", "personal needs", "licenses", "hobbies"}'::text[])
            WITH ORDINALITY
                t2 (alias, i) USING (alias)
            ORDER BY
                t2.i"""
    )
    return [ dict(item) for item in data ]



####################################################################
async def tag_1_category_update(category, allow_user_tags):
    api = get_api_context()
    data = await api.pg.club.execute(
        """UPDATE
                tags_categories
            SET
                allow_user_tags = $2
            WHERE
                alias = $1""",
        category, allow_user_tags
    )



####################################################################
async def tag_1_get_category(category):
    api = get_api_context()
    data = await api.pg.club.fetchrow(
        """SELECT
                alias, name, catalog, allow_user_tags
            FROM
                tags_categories
            WHERE
                alias = $1""",
        category
    )
    if data:
        return {
            'alias': data['alias'],
            'name': data['name'],
            'catalog': data['catalog'],
            'allow_user_tags': data['allow_user_tags'],
        }
    return None



####################################################################
async def tag_1_get_catalog(catalog):
    api = get_api_context()
    data = await api.pg.club.fetch(
        """WITH RECURSIVE r AS (
                    SELECT
                        id, parent_id, tag, 1 AS level, lpad(id::text, 8, '0') AS path, tag AS tag_full
                    FROM
                        tags_catalog
                    WHERE
                        catalog = $1 AND parent_id IS NULL
                UNION ALL
                    SELECT
                        s.id, s.parent_id, s.tag, r.level + 1 AS level, r.path || ':' || lpad(s.id::text, 8, '0') AS path, r.tag_full || ' | ' || s.tag AS tag_full
                    FROM
                        tags_catalog AS s
                    JOIN
                        r ON s.parent_id = r.id
            )
            SELECT
                *
            FROM
                r
            ORDER BY
                path""",
        catalog
    )
    return [ dict(item) for item in data ]



####################################################################
async def tag_1_get_tags(category):
    api = get_api_context()
    result = {}
    data = await api.pg.club.fetch(
        """SELECT
                tags AS tags,
                user_id AS user_id
            FROM
                users_tags_1
            WHERE
                category = $1 AND tags IS NOT NULL AND tags <> ''""",
        category
    )
    for row in data:
        temp = parse_tags_1(row['tags'])
        for k, v in temp.items():
            if k in result:
                result[k]['amount'] += 1
                result[k]['options'].update(v)
                result[k]['users_ids'].add(row['user_id'])
            else:
                result[k] = {
                    'amount': 1,
                    'options': v,
                    'users_ids': { row['user_id'] },
                }
    return result



####################################################################
async def tag_1_catalog_add_tag(id, catalog, tag):
    api = get_api_context()
    data = await api.pg.club.fetch(
        """INSERT INTO
                tags_catalog (parent_id, catalog, tag)
            VALUES
                ($1, $2, $3)""",
        id, catalog, tag
    )



####################################################################
async def tag_1_catalog_update_tag(id, tag):
    api = get_api_context()
    data = await api.pg.club.fetch(
        """UPDATE
                tags_catalog
            SET
                tag = $2
            WHERE
                id = $1""",
        id, tag
    )



####################################################################
async def tag_1_catalog_delete_tag(id):
    api = get_api_context()
    data = await api.pg.club.fetch(
        """DELETE FROM
                tags_catalog
            WHERE
                id = $1""",
        id
    )



####################################################################
async def tag_1_user_update_tag(category, tag, tag_new):
    api = get_api_context()
    if type(tag) == list:
        await asyncio.gather(
            *[
                update_user_tag(api, category, t, tag_new) for t in tag
            ]
        )
    else:
        await update_user_tag(api, category, tag, tag_new)



####################################################################
async def tag_1_user_delete_tag(category, tag):
    api = get_api_context()
    await update_user_tag(api, category, tag, '')



####################################################################
async def update_user_tag(api, category, tag, tag_new):
    await api.pg.club.execute(
        """UPDATE
                users_tags_1
            SET
                tags = tag_replace_1(tags, $2, $3)
            WHERE
                category = $1""",
        category, tag, tag_new
    )
