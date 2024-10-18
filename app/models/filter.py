from app.core.context import get_api_context



####################################################################
async def save_filter(user_id, type, name, filter):
    api = get_api_context()
    await api.pg.club.execute(
        """INSERT INTO manager_filters (user_id, type, name, filter) VALUES ($1, $2, $3, $4) ON CONFLICT (user_id, type, name) DO UPDATE SET filter = EXCLUDED.filter""",
        user_id, type, name, filter
    )



####################################################################
async def load_filter(user_id, type, name):
    api = get_api_context()
    filter = await api.pg.club.fetchval(
        """SELECT filter FROM manager_filters WHERE user_id = $1 AND type = $2 AND name = $3""",
        user_id, type, name
    )
    if filter:
        return filter
    return None



# CREATE TYPE filter_type
#     AS ENUM ('events');

# CREATE TABLE manager_filters (
#     user_id bigint NOT NULL,
#     type filter_type NOT NULL,
#     name text NOT NULL,
#     filter json NOT NULL
# );

# CREATE UNIQUE INDEX manager_filters_pkey ON manager_filters (user_id, type, name);

# ALTER TABLE manager_filters
#     ADD CONSTRAINT manager_filters_user_id_fkey FOREIGN KEY (user_id) REFERENCES users (id) ON UPDATE CASCADE ON DELETE CASCADE;
