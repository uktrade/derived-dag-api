"""A module that generates Airflow DAGS from the dataflow.derived_pipelines table"""
import os
from datetime import datetime, timedelta
from urllib.parse import urlparse

from airflow import DAG
from airflow.api.client import get_current_api_client
from airflow.models import DagModel
from airflow.operators.python import PythonOperator
from sqlalchemy_utils.types.pg_composite import psycopg2


def print_query(query):
    print("*" * 80)
    print("Query fetched from derived_pipelines table")
    print(query)
    print("*" * 80)


class _DerivedSQLDagPipeline:
    paused_on_creation: bool = False
    schedule_interval: str
    query: str

    def get_dag(self) -> DAG:
        dag = DAG(
            self.__class__.__name__,
            catchup=False,
            default_args={
                "owner": "airflow",
                "depends_on_past": False,
                "email_on_failure": False,
                "email_on_retry": False,
                'catchup': False,
            },
            start_date=datetime(2021, 12, 20),
            end_date=None,
            schedule_interval=self.schedule_interval,
            max_active_runs=1,
            is_paused_upon_creation=self.paused_on_creation,
            tags=["Derived DAG pipeline"],
        )
        with dag:
            PythonOperator(
                task_id="print-query-1",
                python_callable=print_query,
                execution_timeout=timedelta(minutes=60),
                provide_context=True,
                op_args=[self.query],
            )
            PythonOperator(
                task_id="print-query-2",
                python_callable=print_query,
                execution_timeout=timedelta(minutes=60),
                provide_context=True,
                op_args=[self.query],
            )
        return dag


def generate_dags():
    """
    Loop through all enabled, non-deleted derived_pipeline records and generate
    DAGs for them dynamically.
    """
    parsed_uri = urlparse(os.environ['AIRFLOW__CORE__SQL_ALCHEMY_CONN'])
    host, port, dbname, user, password = (
        parsed_uri.hostname,
        parsed_uri.port or 5432,
        parsed_uri.path.strip('/'),
        parsed_uri.username,
        parsed_uri.password,
    )
    with psycopg2.connect(
        f'host={host} port={port} dbname={dbname} user={user} password={password}'
    ) as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute("SELECT * FROM derived_dags_plugin.derived_pipelines")
        for pipeline in cursor.fetchall():
            dag_model = DagModel.get_dagmodel(pipeline["dag_id"])
            dag_exists = dag_model is not None
            if not pipeline['deleted']:
                print(f"Generating DAG with id {pipeline['dag_id']}")
                generate_dag(
                    pipeline,
                    dag_exists=dag_exists,
                )
                if dag_exists:
                    dag_model.set_is_paused(is_paused=not pipeline['enabled'])
            elif dag_exists:
                print(f"Deleting DAG with id {pipeline['dag_id']}")
                api_client = get_current_api_client()
                api_client.delete_dag(dag_id=pipeline["dag_id"])


def generate_dag(pipeline, dag_exists):
    if pipeline["type"] == "sql":
        # Dynamically create the dag class
        dag_class = type(
            pipeline["dag_id"],
            (_DerivedSQLDagPipeline,),
            {
                "paused_on_creation": not dag_exists and not pipeline['enabled'],
                "schedule_interval": pipeline["schedule"],
                "query": pipeline["config"]["sql"],
            },
        )
        # Add the class to this module to be picked up by airflow
        globals()[pipeline["dag_id"]] = dag_class().get_dag()
    else:
        raise NotImplementedError(
            f"Unable to configure pipeline with id {pipeline['dag_id']} and type {pipeline['type']}"
        )


generate_dags()
