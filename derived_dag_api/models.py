import enum
from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base(metadata=sa.MetaData(schema="derived_dags_plugin"))


class DerivedPipelineTypes(enum.Enum):
    sql = 'sql'


class DerivedPipelines(Base):
    __tablename__ = 'derived_pipelines'

    id = sa.Column(sa.Integer, primary_key=True)
    dag_id = sa.Column(sa.String(length=255), unique=True)
    type = sa.Column(
        sa.Enum(DerivedPipelineTypes),
        nullable=False,
    )
    schedule = sa.Column(sa.String(length=50))
    schema_name = sa.Column(sa.String(length=63), nullable=False)
    table_name = sa.Column(sa.String(length=63), nullable=False)
    config = sa.Column(JSONB, nullable=False)
    enabled = sa.Column(sa.Boolean, default=False)
    deleted = sa.Column(sa.Boolean, default=False)
    created = sa.Column(sa.DateTime, default=datetime.now)
    modified = sa.Column(sa.DateTime, onupdate=datetime.now)
