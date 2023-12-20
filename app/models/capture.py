import asyncio



################################################################
def capture_signing(api, session_id, user_id, sign_in):
    loop = asyncio.get_event_loop()
    loop.create_task(capture_signing_worker(api, session_id, user_id, sign_in))



################################################################
async def capture_signing_worker(api, session_id, user_id, sign_in):
    await api.pg.club.execute(
        """INSERT INTO signings (session_id, user_id, sign_in) VALUES ($1, $2, $3)""",
        session_id, user_id, sign_in
    )



################################################################
def set_session_settings(api, session_id, client, agent):
    loop = asyncio.get_event_loop()
    loop.create_task(set_session_settings_worker(api, session_id, client, agent))



################################################################
async def set_session_settings_worker(api, session_id, client, agent):
    await api.pg.club.execute(
        """UPDATE sessions SET settings = $1 WHERE id = $2""",
        {
            'client': client,
            'agent': agent,
        },
        session_id
    )
