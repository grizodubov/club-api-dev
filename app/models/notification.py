import asyncio
import re
import orjson
from secrets import token_hex
from jinja2 import Template

from app.core.context import get_api_context
from app.models.community import Community
from app.models.user import User
from app.helpers.telegram import send_telegram_message



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
async def set_subtoken(api, user_id):
    subtoken = ''
    await api.redis.tokens.acquire()
    while True:
        subtoken = token_hex(32)
        result = await api.redis.tokens.exec('EXISTS', subtoken)
        if result == 0:
            break
    await api.redis.tokens.exec('SET', subtoken, str(user_id), ex = 3600)
    api.redis.tokens.release()
    return subtoken



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
    author_id = await api.pg.club.fetchval("""SELECT author_id FROM posts WHERE id = $1""", item_id)
    community = Community()
    await community.set(id = params['community_id'])
    question_tags = None
    if params['reply_to_post_id']:
        question_tags = await api.pg.club.fetchval(
            """SELECT tags FROM posts WHERE id = $1""",
            params['reply_to_post_id']
        )
    else:
        question_tags = await api.pg.club.fetchval(
            """SELECT tags FROM posts WHERE id = $1""",
            item_id
        )
    if community.tags or question_tags:
        tags = question_tags if question_tags else community.tags
        query = ' | '.join([ re.sub(r'\s+', ' & ', t.strip()) for t in tags.split(',') ])
        result = await api.pg.club.fetch(
            """SELECT
                    t1.id, t3.id_telegram
                FROM
                    users t1
                INNER JOIN
                    users_tags t2 ON t2.user_id = t1.id
                INNER JOIN
                    users_info t3 ON t3.user_id = t1.id
                WHERE
                    to_tsvector(t2.tags) @@ to_tsquery($1) OR
                    to_tsvector(t2.interests) @@ to_tsquery($1)""",
            query
        )
        recepients_ids = [ item['id'] for item in result if item['id'] != author_id ]
        telegram_chats = [ (item['id'], item['id_telegram']) for item in result if item['id'] != author_id and item['id_telegram'] ]
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
        link = '/communities' + link + '?sbt=___SUBTOKEN___'
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
            message, link, item_id, [ author_id ]
        )
        link_html = '<a href="https://social.clubgermes.ru' + link + '">Перейти в клуб</a>'
        #print('SENDING NOTIFICATIONS!!!!!!!!')
        send_notification(author_id)
        recepient = User()
        await recepient.set(id = author_id)
        if recepient.id_telegram:
            send_telegram_message(api.stream_telegram, recepient.id_telegram, message + ' ' + link_html)
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
            for chat in telegram_chats:
                subtoken = await set_subtoken(api, chat[0])
                link_html = link_html.replace('___SUBTOKEN___', subtoken)
                send_telegram_message(api.stream_telegram, chat[1], message + ' ' + link_html)



####################################################################
async def process_poll_create(api, user_id, item_id, params):
    TEMPLATES = {
        'poll': 'Добавлен новый опрос в сообщество «{{ community.name }}»',
    }
    community = Community()
    await community.set(id = params['community_id'])
    result = []
    if params['wide']:
        result = await api.pg.club.fetch(
            """SELECT
                    t1.id, t3.id_telegram
                FROM
                    users t1
                INNER JOIN
                    users_info t3 ON t3.user_id = t1.id
                WHERE
                    t1.id >= 10000"""
        )
    elif params['tags']:
        query = ' | '.join([ re.sub(r'\s+', ' & ', t.strip()) for t in tags.split(',') ])
        result = await api.pg.club.fetch(
            """SELECT
                    t1.id, t3.id_telegram
                FROM
                    users t1
                INNER JOIN
                    users_tags t2 ON t2.user_id = t1.id
                INNER JOIN
                    users_info t3 ON t3.user_id = t1.id
                WHERE
                    to_tsvector(t2.tags) @@ to_tsquery($1) OR
                    to_tsvector(t2.interests) @@ to_tsquery($1)""",
            query
        )
    recepients_ids = [ item['id'] for item in result ]
    telegram_chats = [ (item['id'], item['id_telegram']) for item in result if item['id_telegram'] ]
    if recepients_ids:
        link = '/' + str(community.id)
        if community.parent_id:
            link = '/' + str(community.parent_id) + link
        link = '/communities' + link + '?sbt=___SUBTOKEN___'
        link_html = '<a href="https://social.clubgermes.ru' + link + '">Перейти в клуб</a>'
        body = Template(TEMPLATES['poll'])
        message  = body.render(
            community = {
                'name': community.name,
            },
        )
        #print(message)
        await api.pg.club.execute(
            """INSERT INTO notifications (message, link, item_id, recepients) VALUES ($1, $2, $3, $4)""",
            message, link, item_id, recepients_ids
        )
        send_notifications(recepients_ids)
        for chat in telegram_chats:
            subtoken = await set_subtoken(api, chat[0])
            link_html = link_html.replace('___SUBTOKEN___', subtoken)
            print(chat[0], chat[1], message + ' ' + link_html)
            send_telegram_message(api.stream_telegram, chat[1], message + ' ' + link_html)
