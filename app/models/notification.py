import asyncio
import re
import orjson
from secrets import token_hex
from jinja2 import Template

from app.core.context import get_api_context
from app.models.community import Community
from app.models.user import User
from app.models.event import Event
from app.helpers.telegram import send_telegram_message
from app.helpers.email import send_email
from app.helpers.mobile import send_mobile_message
from app.helpers.push import send_push_message



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
async def notifications_read_all(user_id):
    api = get_api_context()
    await api.pg.club.execute('DELETE FROM notifications WHERE item_id NOT IN (SELECT id FROM items_signatures)')
    ids = await api.pg.club.fetchval(
        """SELECT
                array_agg(t1.item_id)
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
    if ids:
        query = []
        args = []
        for i, item in enumerate(ids, 2):
            query.append('($' + str(i) + ', $1)')
            args.append(item)
        data = await api.pg.club.fetch( 
            """INSERT INTO
                    items_views
                    (item_id, user_id)
                VALUES """ + ', '.join(query) + """
                ON CONFLICT
                    (item_id, user_id)
                DO NOTHING""",
            user_id, *ids
        )



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
        'sms': 'Примите участие в жизни клуба и пройдите опрос: "{{ question }}". Ваше мнение поможет нам стать лучше! Подробности в личном кабинете.',
        'email': 'Примите участие в жизни клуба и пройдите опрос: "{{ question }}". Ваше мнение поможет нам стать лучше! Подробности в личном кабинете.',
        'push': '{{ question }}',
    }
    community = Community()
    await community.set(id = params['community_id'])
    result = []
    if params['wide']:
        result = await api.pg.club.fetch(
            """SELECT
                    t1.id, t3.id_telegram, t1.email, t1.phone
                FROM
                    users t1
                INNER JOIN
                    users_info t3 ON t3.user_id = t1.id
                WHERE
                    t1.active IS TRUE AND t1.id >= 10000"""
        )
    elif params['tags']:
        query = ' | '.join([ re.sub(r'\s+', ' & ', t.strip()) for t in tags.split(',') ])
        result = await api.pg.club.fetch(
            """SELECT
                    t1.id, t3.id_telegram, t1.email, t1.phone
                FROM
                    users t1
                INNER JOIN
                    users_tags t2 ON t2.user_id = t1.id
                INNER JOIN
                    users_info t3 ON t3.user_id = t1.id
                WHERE
                    (
                        (
                            to_tsvector(t2.tags) @@ to_tsquery($1) OR
                            to_tsvector(t2.interests) @@ to_tsquery($1)
                        ) OR TRUE
                    )""",
            query
        )
    recepients_ids = [ item['id'] for item in result ]
    telegram_chats = [ (item['id'], item['id_telegram']) for item in result if item['id_telegram'] ]
    emails = [ item['email'] for item in result ]
    phones = [ item['phone'] for item in result ]
    print(recepients_ids)
    #emails = [ 'lebedev@trade.su' ]
    #phones = [ '+79036162847' ]
    if recepients_ids:
        link = '/' + str(community.id)
        # if community.parent_id:
        #     link = '/' + str(community.parent_id) + link
        # else:
        #     link = '/0' + link
        link = '/communities' + link# + '?sbt=___SUBTOKEN___'
        link_html = '<a href="https://social.clubgermes.ru' + link + '">Перейти в клуб</a>'
        # if phones:
        #     body = Template(TEMPLATES['sms'])
        #     message  = body.render(
        #         question = params['text']
        #     )
        #     for t in phones:
        #         send_mobile_message(api.stream_mobile, t, message + ' https://social.clubgermes.ru' + link, {})
        # if emails:
        #     body = Template(TEMPLATES['email'])
        #     message  = body.render(
        #         question = params['text']
        #     )
        #     for t in emails:
        #         send_email(api.stream_email, t, 'Клуб Гермес: Новый опрос', message + '<br />' + link_html, {})
        body = Template(TEMPLATES['push'])
        message = body.render(
            question = params['text'][0:30] + '...'
        )
        send_push_message(api, recepients_ids, 'Новый опрос', message, link)



####################################################################
async def process_return_to_agent(api, user_id, item_id, params):
    TEMPLATES = {
        'agent_notification': 'Соискатель {{ client_name }} возвращён Вам на уточнение данных',
        'agent_sms': 'Клуб Гермес. Соискатель {{ client_name }} возвращён Вам на уточнение данных. Подробности в личном кабинете агента.',
        'agent_email': 'Клуб Гермес. Работа по соискателю {{ client_name }} приостановлена. Соискатель {{ client_name }} возвращён Вам на уточнение данных. Информация о причине приостановки и порядок возобновления работы доступны в личном кабинете агента.',
    }
    data = await api.pg.club.fetchrow(
        """SELECT
                t1.id AS client_id, t1.name AS client_name,
                t2.id AS community_manager_id, t2.name AS community_manager_name, t2.email AS community_manager_email,
                t2.phone AS community_manager_phone, t2.phone AS community_manager_phone, t2_i.id_telegram AS community_manager_telegram_id,
                t3.id AS agent_id, t3.name AS agent_name, t3.email AS agent_email,
                t3.phone AS agent_phone, t3_i.id_telegram AS agent_telegram_id
            FROM
                users t1
            LEFT JOIN
                users t2 ON t2.id = t1.community_manager_id
            LEFT JOIN
                users_info t2_i ON t2_i.user_id = t2.id
            LEFT JOIN
                users t3 ON t3.id = t1.agent_id
            LEFT JOIN
                users_info t3_i ON t3_i.user_id = t3.id
            WHERE
                t1.id = $1""",
        item_id
    )
    if data and data['agent_id']:
        link = 'https://manager.clubgermes.ru/users/' + str(item_id)
        body = Template(TEMPLATES['agent_notification'])
        message  = body.render(
            client_name = data['client_name']
        )
        await api.pg.club.execute(
            """INSERT INTO notifications (message, link, item_id, recepients) VALUES ($1, $2, $3, $4)""",
            message, link, item_id, [ data['agent_id'] ]
        )
        send_notifications([ data['agent_id'] ])
        link = link + '?sbt=___SUBTOKEN___'
        link_html = '<a href="' + link + '">Перейти в клуб</a>'

        subtoken = await set_subtoken(api, data['agent_id'])
        link_html = link_html.replace('___SUBTOKEN___', subtoken)

        if data['agent_telegram_id']:
            send_telegram_message(api.stream_telegram, data['agent_telegram_id'], message + ' ' + link_html)

        body = Template(TEMPLATES['agent_sms'])
        message  = body.render(
            client_name = data['client_name']
        )
        if data['agent_phone']:
            send_mobile_message(api.stream_mobile, data['agent_phone'], message + ' ' + link_html, {})

        body = Template(TEMPLATES['agent_email'])
        message  = body.render(
            client_name = data['client_name']
        )
        if data['agent_email']:
            send_email(api.stream_email, data['agent_email'], 'Гермес: возвращен соискатель ' + data['client_name'], message + '<br />' + link_html, {})



####################################################################
async def process_return_to_manager(api, user_id, item_id, params):
    TEMPLATES = {
        'manager_notification': 'Агент {{ agent_name }} запрашивает возврат в работу соискателя {{ client_name }}',
        'manager_sms': 'Клуб Гермес. Агент {{ agent_name }} запрашивает возврат в работу соискателя {{ client_name }}. Подробности в Личном кабинете КМ.',
        'manager_email': 'Клуб Гермес. Агент {{ agent_name }} запрашивает возврат в работу соискателя {{ client_name }}. Подробности в Личном кабинете КМ.',
    }
    data = await api.pg.club.fetchrow(
        """SELECT
                t1.id AS client_id, t1.name AS client_name,
                t2.id AS community_manager_id, t2.name AS community_manager_name, t2.email AS community_manager_email,
                t2.phone AS community_manager_phone, t2.phone AS community_manager_phone, t2_i.id_telegram AS community_manager_telegram_id,
                t3.id AS agent_id, t3.name AS agent_name, t3.email AS agent_email,
                t3.phone AS agent_phone, t3_i.id_telegram AS agent_telegram_id
            FROM
                users t1
            LEFT JOIN
                users t2 ON t2.id = t1.community_manager_id
            LEFT JOIN
                users_info t2_i ON t2_i.user_id = t2.id
            LEFT JOIN
                users t3 ON t3.id = t1.agent_id
            LEFT JOIN
                users_info t3_i ON t3_i.user_id = t3.id
            WHERE
                t1.id = $1""",
        item_id
    )
    if data and data['community_manager_id']:
        link = 'https://manager.clubgermes.ru/users/' + str(item_id)
        body = Template(TEMPLATES['manager_notification'])
        message  = body.render(
            client_name = data['client_name']
        )
        await api.pg.club.execute(
            """INSERT INTO notifications (message, link, item_id, recepients) VALUES ($1, $2, $3, $4)""",
            message, link, item_id, [ data['community_manager_id'] ]
        )
        send_notifications([ data['community_manager_id'] ])
        link = link + '?sbt=___SUBTOKEN___'
        link_html = '<a href="' + link + '">Перейти в клуб</a>'

        subtoken = await set_subtoken(api, data['community_manager_id'])
        link_html = link_html.replace('___SUBTOKEN___', subtoken)

        if data['community_manager_telegram_id']:
            send_telegram_message(api.stream_telegram, data['community_manager_telegram_id'], message + ' ' + link_html)

        body = Template(TEMPLATES['manager_sms'])
        message  = body.render(
            client_name = data['client_name']
        )
        if data['community_manager_phone']:
            send_mobile_message(api.stream_mobile, data['community_manager_phone'], message + ' ' + link, {})

        body = Template(TEMPLATES['manager_email'])
        message  = body.render(
            client_name = data['client_name']
        )
        if data['community_manager_email']:
            send_email(api.stream_email, data['community_manager_email'], 'Гермес: возвращен соискатель ' + data['client_name'], message + '<br />' + link_html, {})



####################################################################
async def process_rating_poll_create(api, user_id, item_id, params):
    TEMPLATES = {
        'sms': 'Примите участие в жизни клуба и пройдите опрос: "{{ question }}". Ваше мнение поможет нам стать лучше! Подробности в личном кабинете.',
        'email': 'Примите участие в жизни клуба и пройдите опрос: "{{ question }}". Ваше мнение поможет нам стать лучше! Подробности в личном кабинете.',
    }
    data = await api.pg.club.fetchrow(
        """SELECT
                array_agg(t1.phone) AS phones, array_agg(t1.email) AS emails
            FROM
                users t1
            INNER JOIN
                users_roles t2 ON t2.user_id = t1.id
            INNER JOIN
                roles t3 ON t3.id = t2.role_id
            WHERE
                t3.alias = 'client'"""
    )
    link = 'https://social.clubgermes.ru/'
    link_html = '<a href="' + link + '">Перейти в клуб</a>'
    if data['phones']:
        body = Template(TEMPLATES['sms'])
        message  = body.render(
            question = params['text']
        )
        #print(data['phones'])
        for t in data['phones']:
        #for t in [ '+79036162847' ]:
            send_mobile_message(api.stream_mobile, t, message + ' ' + link, {})
    if data['emails']:
        body = Template(TEMPLATES['email'])
        message  = body.render(
            question = params['text']
        )
        #print(data['emails'])
        for t in data['emails']:
        #for t in [ 'lebedev@trade.su' ]:
            send_email(api.stream_email, t, 'Клуб Гермес: Новый опрос', message + '<br />' + link_html, {})



####################################################################
async def process_connection_add(api, user_id, item_id, params):
    TEMPLATES = {
        'sms_target': 'Вам предложил личную встречу {{initiator}}{% if initiator_company %} ({{initiator_company}}){% endif %}',
        'push_target': 'Вам предложил личную встречу {{initiator}}{% if initiator_company %} ({{initiator_company}}){% endif %}',
        'sms_manager': '{{initiator}}{% if initiator_company %} ({{initiator_company}}){% endif %} предложил личную встречу клиенту {{target}}{% if target_company %} ({{target_company}}){% endif %}',
        'push_manager': '{{initiator}}{% if initiator_company %} ({{initiator_company}}){% endif %} предложил личную встречу клиенту {{target}}{% if target_company %} ({{target_company}}){% endif %}',
    }
    user_initiator = User()
    await user_initiator.set(id = user_id)
    user_target = User()
    await user_target.set(id = params['user_id'])
    event = Event()
    await event.set(id = item_id)
    manager_initiator = User()
    if user_initiator.community_manager_id:
        await manager_initiator.set(id = user_initiator.community_manager_id)
    manager_target = User()
    if user_target.community_manager_id and user_target.community_manager_id != user_initiator.community_manager_id:
        await manager_target.set(id = user_target.community_manager_id)
    if user_target.id and user_initiator.id and event.id:
        link = '/residents/' + str(user_initiator.id)

        body = Template(TEMPLATES['push_target'])
        message = body.render(
            initiator = user_initiator.name,
            initiator_company = user_initiator.company,
        )
        send_push_message(api, [ user_target.id ], 'Назначение встречи', message, link)

        body = Template(TEMPLATES['sms_target'])
        message  = body.render(
            initiator = user_initiator.name,
            initiator_company = user_initiator.company,
        )
        if user_target.phone:
            send_mobile_message(api.stream_mobile, user_target.phone, message, {})
        
        if manager_initiator.id or manager_target.id:
            recepients = []
            if manager_initiator.id:
                recepients.append(manager_initiator.id)
            if manager_target.id:
                recepients.append(manager_target.id)

            body = Template(TEMPLATES['push_manager'])
            message = body.render(
                initiator = user_initiator.name,
                initiator_company = user_initiator.company,
                target = user_target.name,
                target_company = user_target.company,
            )
            send_push_message(api, recepients, 'Назначение встречи', message, link)

            body = Template(TEMPLATES['sms_manager'])
            message = body.render(
                initiator = user_initiator.name,
                initiator_company = user_initiator.company,
                target = user_target.name,
                target_company = user_target.company,
            )
        
            if manager_initiator.phone:
                send_mobile_message(api.stream_mobile, manager_initiator.phone, message, {})
            
            if manager_target.phone:
                send_mobile_message(api.stream_mobile, manager_target.phone, message, {})



####################################################################
async def process_connection_response(api, user_id, item_id, params):
    TEMPLATES = {
        'sms_target': '{{initiator}}{% if initiator_company %} ({{initiator_company}}){% endif %} {% if resp %}согласился с Вашим предложением{% else %}отклонил Ваше предложение{% endif %} о личной встрече',
        'push_target': '{{initiator}}{% if initiator_company %} ({{initiator_company}}){% endif %} {% if resp %}согласился с Вашим предложением{% else %}отклонил Ваше предложение{% endif %} о личной встрече',
        'sms_manager': '{{initiator}}{% if initiator_company %} ({{initiator_company}}){% endif %} {% if resp %}согласился с предложением{% else %}отклонил предложение{% endif %} о личной встрече с клиентом {{target}}{% if target_company %} ({{target_company}}){% endif %}',
        'push_manager': '{{initiator}}{% if initiator_company %} ({{initiator_company}}){% endif %} {% if resp %}согласился с предложением{% else %}отклонил предложение{% endif %} о личной встрече с клиентом {{target}}{% if target_company %} ({{target_company}}){% endif %}',
    }
    user_initiator = User()
    await user_initiator.set(id = user_id)
    user_target = User()
    await user_target.set(id = params['user_id'])
    event = Event()
    await event.set(id = item_id)
    manager_initiator = User()
    if user_initiator.community_manager_id:
        await manager_initiator.set(id = user_initiator.community_manager_id)
    manager_target = User()
    if user_target.community_manager_id and user_target.community_manager_id != user_initiator.community_manager_id:
        await manager_target.set(id = user_target.community_manager_id)
    if user_target.id and user_initiator.id and event.id:
        link = '/residents/' + str(user_initiator.id)

        body = Template(TEMPLATES['push_target'])
        message = body.render(
            initiator = user_initiator.name,
            initiator_company = user_initiator.company,
            resp = params['response'],
        )
        send_push_message(api, [ user_target.id ], 'Назначение встречи', message, link)

        body = Template(TEMPLATES['sms_target'])
        message  = body.render(
            initiator = user_initiator.name,
            initiator_company = user_initiator.company,
            resp = params['response'],
        )
        if user_target.phone:
            send_mobile_message(api.stream_mobile, user_target.phone, message, {})
        
        if manager_initiator.id or manager_target.id:
            recepients = []
            if manager_initiator.id:
                recepients.append(manager_initiator.id)
            if manager_target.id:
                recepients.append(manager_target.id)

            body = Template(TEMPLATES['push_manager'])
            message = body.render(
                initiator = user_initiator.name,
                initiator_company = user_initiator.company,
                target = user_target.name,
                target_company = user_target.company,
                resp = params['response'],
            )
            send_push_message(api, recepients, 'Назначение встречи', message, link)

            body = Template(TEMPLATES['sms_manager'])
            message = body.render(
                initiator = user_initiator.name,
                initiator_company = user_initiator.company,
                target = user_target.name,
                target_company = user_target.company,
                resp = params['response'],
            )
        
            if manager_initiator.phone:
                send_mobile_message(api.stream_mobile, manager_initiator.phone, message, {})
            
            if manager_target.phone:
                send_mobile_message(api.stream_mobile, manager_target.phone, message, {})



####################################################################
async def process_user_arrive(api, user_id, item_id, params):
    TEMPLATES = {
        'push_target': '{{user}}{% if user_company %} ({{user_company}}){% endif %} прибыл на мероприятие (назначена встреча)',
        'sms_manager': '{{user}}{% if user_company %} ({{user_company}}){% endif %} прибыл на мероприятие',
        'push_manager': '{{user}}{% if user_company %} ({{user_company}}){% endif %} прибыл на мероприятие',
    }
    user = User()
    await user.set(id = item_id)
    manager = User()
    if user.community_manager_id:
        await manager.set(id = user.community_manager_id)
    data = await api.pg.club.fetch(
        """SELECT
                user_1_id, user_2_id
            FROM
                users_connections
            WHERE
                (user_1_id = $1 OR user_2_id = $1) AND
                event_id = $2 AND
                deleted IS FALSE""",
        user.id, params['event_id']
    )
    temp = []
    if data:
        for item in data:
            if item['user_1_id'] != user.id:
                temp.append(item['user_1_id'])
            else:
                temp.append(item['user_2_id'])
    if temp:
        link = '/residents/' + str(user.id)

        body = Template(TEMPLATES['push_target'])
        message = body.render(
            user = user.name,
            user_company = user.company,
        )
        send_push_message(api, temp, 'Прибытие на мероприятие', message, link)
    
    if manager.id:

        body = Template(TEMPLATES['push_manager'])
        message = body.render(
            user = user.name,
            user_company = user.company,
        )
        send_push_message(api, [ manager.id ], 'Прибытие на мероприятие', message, link)

        body = Template(TEMPLATES['sms_manager'])
        message = body.render(
            user = user.name,
            user_company = user.company,
        )
    
        if manager.phone:

            body = Template(TEMPLATES['sms_manager'])
            message = body.render(
                user = user.name,
                user_company = user.company,
            )
            send_mobile_message(api.stream_mobile, manager.phone, message, {})
