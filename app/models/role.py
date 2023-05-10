from app.core.context import get_api_context



async def get_roles():
    api = get_api_context()
    roles = await api.pg.club.fetch(
        """SELECT id, alias FROM roles"""
    )
    return {
        role['alias']: role['id'] for role in roles
    }
