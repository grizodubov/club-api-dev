from app.core.context import get_api_context

from app.models.news import News
from app.models.event import Event



async def get_feed():
    api = get_api_context()
    result = []
    data = await api.pg.club.fetch(
        """SELECT
                t5.id, t5.time_create, t5.time_update,
                t5.name, t5.format, t5.place, t5.time_event, t5.time_news,
                t5.detail, t5.active,
                t5.thumbs_up
            FROM
            (
                SELECT
                    t1.id, t1.time_create, t1.time_update,
                    t1.name, t1.format, t1.place, t1.time_event, NULL AS time_news,
                    t1.detail, t1.active, t1.time_event AS time_sort,
                    coalesce(t2.thumbs_up, 0) AS thumbs_up
                FROM
                    events t1
                LEFT JOIN
                    (SELECT item_id, count(user_id) AS thumbs_up FROM items_thumbsup GROUP BY item_id) t2 ON t2.item_id = t1.id
                WHERE
                    t1.active IS TRUE

                UNION ALL

                SELECT
                        t3.id, t3.time_create, t3.time_update,
                        t3.name, NULL AS format, NULL AS place, NULL AS time_event, t3.time_news,
                        t3.detail, t3.active, t3.time_news AS time_sort,
                        coalesce(t4.thumbs_up, 0) AS thumbs_up
                    FROM
                        news t3
                    LEFT JOIN
                        (SELECT item_id, count(user_id) AS thumbs_up FROM items_thumbsup GROUP BY item_id) t4 ON t4.item_id = t3.id
            ) t5
            ORDER BY
                t5.time_sort DESC
            LIMIT 50"""
    )
    for row in data:
        if row['time_event'] is not None:
            item = Event()
            item.__dict__ = { k: row[k] for k in row.keys() if k in { 'id', 'time_create', 'time_update', 'name', 'format', 'place', 'time_event', 'detail', 'active', 'thumbs_up' } }
        else:
            item = News()
            item.__dict__ = { k: row[k] for k in row.keys() if k in { 'id', 'time_create', 'time_update', 'name', 'time_news', 'detail', 'active', 'thumbs_up' } }
        item.check_icon()
        item.check_image()
        item.check_files()
        result.append(item)
    return result
