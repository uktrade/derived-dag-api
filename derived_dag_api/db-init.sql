create schema if not exists derived_dags_plugin;
create table if not exists derived_dags_plugin.derived_pipelines
(
    id serial primary key,
    dag_id varchar(255) unique,
    type varchar(255) not null,
    schedule varchar(50),
    schema_name varchar(63) not null,
    table_name  varchar(63) not null,
    config jsonb not null,
    enabled boolean,
    deleted boolean,
    created timestamp,
    modified timestamp
);