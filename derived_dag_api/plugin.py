import os
from http.client import NOT_FOUND

import psycopg2
from airflow.config_templates.airflow_local_settings import FILENAME_TEMPLATE
from airflow.configuration import conf
from airflow.models import DagModel, clear_task_instances
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.amazon.aws.log.s3_task_handler import S3TaskHandler
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState
from airflow.www.api.experimental.endpoints import (
    api_experimental,
    requires_authentication,
)
from flask import abort, jsonify, request
from sqlalchemy.exc import IntegrityError

from .models import DerivedPipelines
from .schemas import DerivedDagInputSchema
from .utils import (
    bad_request_response,
    collect_dags,
    derived_dag_exists,
    get_database_uri,
)

derived_dag_schema = DerivedDagInputSchema()


class DerivedDagApiPlugin(AirflowPlugin):
    name = "derived_dag_api"
    flask_blueprints = [api_experimental]

    def on_load(*args, **kwargs):
        print('Initialising tables for derived dag api')
        sql_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'db-init.sql')
        with psycopg2.connect(get_database_uri()) as conn, conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
        ) as cursor:
            with open(sql_file) as fh:
                query = fh.read()
                cursor.execute(query)
        print('Finished initialising derived dag api tables')


@api_experimental.route('/derived-dags/test', methods=['GET'])
@requires_authentication
def derived_dags_test():
    return jsonify(status="Derived dags plugin loaded OK")


@api_experimental.route('/derived-dags/dag/<string:dag_id>', methods=['DELETE', 'POST', 'PUT'])
@requires_authentication
@provide_session
def derived_dags_dag(dag_id, session):
    dag = session.query(DerivedPipelines).filter(DerivedPipelines.dag_id == dag_id).first()
    dag_exists = dag is not None

    if request.method in ["DELETE", "PUT"] and not dag_exists:
        abort(bad_request_response(f"No DAG exists with the id '{dag_id}", NOT_FOUND))

    if request.method == "DELETE":
        dag.deleted = True
        session.commit()
        collect_dags()
        return jsonify(status=f"DAG '{dag_id}' deleted successfully")

    data = request.get_json(force=True)
    errors = derived_dag_schema.validate(data)
    if errors:
        abort(bad_request_response(str(errors)))

    if request.method == "POST":
        if dag_exists:
            abort(bad_request_response(f"DAG with id '{dag_id}' already exists"))
        dag = DerivedPipelines(dag_id=dag_id)

    for k, v in data.items():
        setattr(dag, k, v)

    try:
        session.add(dag)
        session.commit()
    except IntegrityError:
        abort(
            bad_request_response(
                "Integrity error: Check that the DAG ID doesn't already exist"
            )
        )
    collect_dags()
    return jsonify(
        status=f"DAG {dag_id} {'updated' if dag_exists else 'created'} successfully"
    )


@api_experimental.route('/derived-dags/dag/<string:dag_id>/logs', methods=['GET'])
@requires_authentication
@provide_session
def derived_dags_dag_log(dag_id, session):
    """
    Returns concatenated logs for each task of a DAG's last run
    """
    dag = DagModel.get_current(dag_id)
    if dag is None:
        abort(bad_request_response(f"No DAG exists with the id '{dag_id}", NOT_FOUND))

    last_run = dag.get_last_dagrun(session=session, include_externally_triggered=True)
    if last_run is None:
        return jsonify(logs=[])

    task_handler = S3TaskHandler(
        conf.get("logging", "BASE_LOG_FOLDER"),
        conf.get("logging", "REMOTE_BASE_LOG_FOLDER"),
        FILENAME_TEMPLATE
    )
    log_data = []
    for ti in last_run.get_task_instances():
        if ti.start_date is not None:
            log_data.append({
                "task_id": ti.task_id,
                "logs": task_handler._read(
                    ti,
                    try_number=ti.try_number - 1,
                    metadata={},
                )[0].splitlines()
            })
    return jsonify(log_data)


@api_experimental.route('/derived-dags/dags', methods=['GET'])
@requires_authentication
@provide_session
def derived_dags_dags(session):
    """
    Returns details and last run status for all non-deleted derived dags
    """
    response = {}
    for derived_dag in session.query(DerivedPipelines).filter(DerivedPipelines.deleted == False):
        dag = DagModel.get_current(derived_dag.dag_id)
        last_run = (
           dag.get_last_dagrun(session=session, include_externally_triggered=True)
           if dag is not None else None
        )
        response[derived_dag.dag_id] = {
            "type": derived_dag.type.value,
            "schedule": derived_dag.schedule,
            "schema_name": derived_dag.schema_name,
            "table_name": derived_dag.table_name,
            "enabled": derived_dag.enabled,
            "in_dagbag": dag is not None,
            "last_run": ({
                "run_type": last_run.run_type,
                "queued_at": last_run.queued_at,
                "execution_date": last_run.execution_date,
                "start_date": last_run.start_date,
                "end_date": last_run.end_date,
                "state": last_run.get_state(),
            }) if last_run is not None else None
        }
    return jsonify(response)


@api_experimental.route('/derived-dags/dag/<string:dag_id>/run', methods=['POST'])
@requires_authentication
@provide_session
def derived_dags_dag_run(dag_id, session):
    if not derived_dag_exists(session, dag_id):
        abort(bad_request_response(f"No DAG exists with the id '{dag_id}", NOT_FOUND))
    dag = DagModel.get_current(dag_id)
    if dag is None:
        return jsonify(status=f"No enabled DAG with '{dag_id}'")
    last_run = dag.get_last_dagrun(session=session, include_externally_triggered=True)
    if last_run is None:
        return jsonify(status=f"DAG has not run yet. Please wait until the first run has completed")
    task_instances = last_run.get_task_instances()
    clear_task_instances(task_instances, session)
    return jsonify(status=f"DAG '{dag_id}' started successfully")


@api_experimental.route('/derived-dags/dag/<string:dag_id>/stop', methods=['POST'])
@requires_authentication
@provide_session
def derived_dags_dag_stop(dag_id, session):
    if not derived_dag_exists(session, dag_id):
        abort(bad_request_response(f"No DAG exists with the id '{dag_id}", NOT_FOUND))
    dag = DagModel.get_current(dag_id)
    if dag is None:
        return jsonify(status=f"No enabled DAG with '{dag_id}'")
    last_run = dag.get_last_dagrun(session=session, include_externally_triggered=True)
    if last_run is None:
        return jsonify(status=f"No running tasks for DAG '{dag_id}'")
    last_run.set_state(DagRunState.FAILED)
    return jsonify(status=f"DAG '{dag_id}' stopped successfully")
