from app.core.context import get_api_context

from app.helpers.push import send_push_message



################################################################
async def send_message(user_id, flag_email, flag_sms, flag_push, title, message, link):
    api = get_api_context()
    # data = await api.pg.club.fetchrow(
    #     """SELECT
    #             array_agg(t1.id) AS ids
    #         FROM
    #             users t1
    #         INNER JOIN
    #             users_roles t2 ON t2.user_id = t1.id
    #         INNER JOIN
    #             roles t3 ON t3.id = t2.role_id
    #         WHERE
    #             t1.active IS TRUE AND t3.alias = 'client'"""
    # )
    # data = await api.pg.club.fetchrow(
    #     """SELECT
    #             array_agg(t1.id) AS ids
    #         FROM
    #             users t1
    #         WHERE
    #             t1.id >= 10000 AND t1.active IS TRUE"""
    # )
    data = await api.pg.club.fetchrow(
        """SELECT
                array_agg(t1.id) AS ids
            FROM
                users t1
            INNER JOIN
                events_users t2 ON t2.user_id = t1.id
            WHERE
                t1.id >= 10000 AND t1.active IS TRUE AND t2.event_id = 12833"""
    )
    if data and data['ids']:
        ids = data['ids']
        #ids = [ 10004 ]
        send_push_message(api, ids, title, message, link)
    await save_message(user_id, flag_email, flag_sms, flag_push, title, message, link, ids)



################################################################
async def save_message(user_id, flag_email, flag_sms, flag_push, title, message, link, recepients):
    api = get_api_context()
    await api.pg.club.execute(
        """INSERT INTO send (author_id, flag_email, flag_sms, flag_push, title, message, link, recepients) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)""",
        user_id, flag_email, flag_sms, flag_push, title, message, link, recepients
    )
