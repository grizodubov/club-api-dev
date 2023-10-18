import asyncio
import re
import orjson
from jinja2 import Template

from app.core.context import get_api_context
from app.models.community import Community



####################################################################
def create_notifications(event, user_id, item_id, params):
    api = get_api_context()
    if 'process_' + event in globals():
        asyncio.create_task(globals()['process_' + event](api, user_id, item_id, params))



####################################################################
def send_notification(user_id):
    api = get_api_context()
    asyncio.create_task(send_notification_ws(api, user_id))



####################################################################
async def send_notification_ws(api, user_id):
    await api.websocket_send(user_id, orjson.dumps({ 'notify': True }).decode())



####################################################################
def send_notifications(users_ids):
    api = get_api_context()
    api.websocket_limited_send(users_ids, { 'notify': True })



####################################################################
async def get_notifications(user_id, before = None):
    api = get_api_context()
    if before:
        result = await api.pg.club.fetch(
            """SELECT
                    t1.time_notify, t1.message, t1.link, t2.time_view
                FROM
                    notifications t1
                LEFT JOIN
                    items_views t2 ON t2.item_id = t1.item_id AND t2.user_id = $1
                WHERE
                    $1 = ANY(t1.recepients) AND t1.time_notify < $2
                ORDER BY
                    t1.time_notify DESC
                LiMIT 20""",
            user_id, before
        )
    else:
        result = await api.pg.club.fetch(
            """SELECT
                    t1.time_notify, t1.message, t1.link, t2.time_view
                FROM
                    notifications t1
                LEFT JOIN
                    items_views t2 ON t2.item_id = t1.item_id AND t2.user_id = $1
                WHERE
                    $1 = ANY(t1.recepients)
                ORDER BY
                    t1.time_notify DESC
                LiMIT 50""",
            user_id
        )
    return [ dict(item) for item in result ]



####################################################################
async def get_highlights(user_id):
    api = get_api_context()
    highlights = await api.pg.club.fetchval(
        """SELECT
                count(*)
            FROM
                notifications t1
            WHERE
                NOT EXISTS (
                    SELECT
                        t2.item_id
                    FROM
                        items_views t2
                    WHERE
                        t2.item_id = t1.item_id AND t2.user_id = $1
                ) AND
                $1 = ANY(t1.recepients)""",
        user_id
    )
    return highlights



####################################################################
# METHODS
####################################################################

####################################################################
async def process_post_add(api, user_id, item_id, params):
    TEMPLATES = {
        'question': 'Добавлен новый вопрос в сообщество «{{ community.name }}»',
        'question_self': 'Вы добавили новый вопрос в сообщество «{{ community.name }}»',
        'answer': 'Добавлен новый ответ на вопрос «{{ question.text }}» в сообществе «{{ community.name }}»',
        'answer_self': 'Вы добавили новый ответ на вопрос «{{ question.text }}» в сообществе «{{ community.name }}»',
    }
    community = Community()
    await community.set(id = params['community_id'])
    query = ' | '.join([ re.sub(r'\s+', ' & ', t.strip()) for t in community.tags.split(',') ])
    result = await api.pg.club.fetch(
        """SELECT
                t1.id
            FROM
                users t1
            INNER JOIN
                users_tags t2 ON t2.user_id = t1.id
            WHERE
                to_tsvector(t2.tags) @@ to_tsquery($1) OR
                to_tsvector(t2.interests) @@ to_tsquery($1)""",
        query
    )
    recepients_ids = [ item['id'] for item in result if item['id'] != user_id ]
    template_name = 'answer' if params['reply_to_post_id'] else 'question'
    question_text = None
    if params['reply_to_post_id']:
        question_text = await api.pg.club.fetchval(
            """SELECT text FROM posts WHERE id = $1""",
            params['reply_to_post_id']
        )
        if len(question_text) > 24:
            question_text = question_text[:24] + ' …'
    link = '/' + str(community.id) + '/' + str(item_id)
    if community.parent_id:
        link = '/' + str(community.parent_id) + link
    link = '/communities' + link
    body = Template(TEMPLATES[template_name + '_self'])
    message  = body.render(
        community = {
            'name': community.name,
        },
        question = {
            'text': question_text,
        },
    )
    await api.pg.club.execute(
        """INSERT INTO notifications (message, link, item_id, recepients) VALUES ($1, $2, $3, $4)""",
        message, link, item_id, [ user_id ]
    )
    send_notification(user_id)
    if recepients_ids:
        body = Template(TEMPLATES[template_name])
        message = body.render(
            community = {
                'name': community.name,
            },
            question = {
                'text': question_text,
            },
        )
        await api.pg.club.execute(
            """INSERT INTO notifications (message, link, item_id, recepients) VALUES ($1, $2, $3, $4)""",
            message, link, item_id, recepients_ids
        )
        send_notifications(recepients_ids)
