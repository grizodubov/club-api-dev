from app.core.context import get_api_context



async def get_messages(user_id, chat_id = None):
    data = await api.pg.club.fetch( 
        """SELECT
                array_agg(t1.id) ids
            FROM
                messages t1
            WHERE
                t1.author_id = $1 OR
                t1.target_id = $1 OR
                target_id IN (
                    SELECT group_id FROM groups_users WHERE user_id = $1
                )            """
    )





SELECT t1.id, t1.time_create, t1.author_id, t1.target_id, t1.target_model, t1.text
FROM
(
    SELECT
        r.*,
        ROW_NUMBER() OVER(PARTITION BY r.[SectionID]
                          ORDER BY r.[DateEntered] DESC) rn
    FROM [Records] r
) t1
WHERE 
    t1.author_id = $1 OR
    t1.target_id = $1 OR
    t1.target_id IN (
        SELECT group_id FROM groups_users WHERE user_id = $1
    )
ORDER BY t1.time_create DESC





 id                          | bigint                      |           | not null | 
 time_create                 | timestamp without time zone |           | not null | (now() AT TIME ZONE 'utc'::text)
 time_update                 | timestamp without time zone |           |          | 
 author_id                   | bigint                      |           |          | 
 author_id_deleted           | bigint                      |           |          | 
 target_id                   | bigint                      |           | not null | 
 target_id_deleted           | bigint                      |           |          | 
 target_model                | model                       |           |          | 
 reply_to_message_id         | bigint                      |           |          | 
 reply_to_message_id_deleted | bigint                      |           |          | 
 text                        | text                        |           | not null | 
