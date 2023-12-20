import asyncio



################################################################
def capture_signing(api, session_id, user_id, sign_in):
    print('capture!', sign_in)
    loop = asyncio.get_event_loop()
    loop.create_task(capture_signing_worker(api, session_id, user_id, sign_in))



################################################################
async def capture_signing_worker(api, session_id, user_id, sign_in):
    await api.pg.club.execute(
        """INSERT INTO signings (session_id, user_id, sign_in) VALUES ($1, $2, $3)""",
        session_id, user_id, sign_in
    )
