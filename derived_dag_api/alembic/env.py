import os

from airflow.models import Base
from alembic import context
from sqlalchemy import create_engine

target_metadata = Base.metadata
schema_name = "derived_dags_plugin"


def include_object(object_, name, type_, reflected, compare_to):
    if type_ == 'table' and object_.schema != schema_name:
        return False

    return True


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = os.environ['AIRFLOW__CORE__SQL_ALCHEMY_CONN']
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        version_table_schema=schema_name,
        include_object=include_object,
        include_schemas=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = create_engine(os.environ['AIRFLOW__CORE__SQL_ALCHEMY_CONN'])

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table_schema=schema_name,
            include_object=include_object,
            include_schemas=True,
        )

        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        with context.begin_transaction():
            context.run_migrations()


run_migrations_online()
