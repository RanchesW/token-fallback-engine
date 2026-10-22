import json

import asyncpg


async def _init_connection(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")


async def create_pool(dsn: str, max_size: int) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn, min_size=1, max_size=max_size, init=_init_connection)
