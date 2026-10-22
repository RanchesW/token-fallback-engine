import os

from alembic import context
from sqlalchemy import create_engine, pool


def _sqlalchemy_url() -> str:
    # приложение ходит через asyncpg с обычным postgresql://, а алембику нужен psycopg
    url = os.environ.get("DATABASE_URL", "postgresql://app:app@localhost:5432/app")
    return url.replace("postgresql://", "postgresql+psycopg://", 1)


def run_migrations_online() -> None:
    engine = create_engine(_sqlalchemy_url(), poolclass=pool.NullPool)
    with engine.connect() as connection:
        context.configure(connection=connection, transaction_per_migration=True)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    context.configure(url=_sqlalchemy_url(), literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()
else:
    run_migrations_online()
