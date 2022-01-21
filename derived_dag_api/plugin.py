import os
from http.client import NOT_FOUND

from airflow.models import DagModel
from airflow.plugins_manager import AirflowPlugin
from airflow.settings import SQL_ALCHEMY_CONN
from airflow.utils.log.log_reader import TaskLogReader
from airflow.utils.session import provide_session
from airflow.www.api.experimental.endpoints import (
    api_experimental,
    requires_authentication,
)
from alembic import command
from flask import abort, current_app, jsonify, request
from sqlalchemy.exc import IntegrityError

from .models import DerivedPipelines
from .schemas import DerivedDagInputSchema
from .utils import bad_request_response, collect_dags

derived_dag_schema = DerivedDagInputSchema()


class DerivedDagApiPlugin(AirflowPlugin):
    name = "derived_dag_api"
    flask_blueprints = [api_experimental]

    def on_load(*args, **kwargs):
        from alembic.config import Config

        print('Running migrations for derived dag api')
        current_dir = os.path.dirname(os.path.abspath(__file__))
        print(f"Current dir is {current_dir}")
        alembic_dir = os.path.join(current_dir, 'alembic')
        print(f"Alembic directory is {alembic_dir}")
        config = Config(os.path.join(alembic_dir, 'alembic.ini'))
        print('Alembic config', vars(config))
        config.set_main_option('script_location', alembic_dir.replace('%', '%%'))
        config.set_main_option('sqlalchemy.url', SQL_ALCHEMY_CONN.replace('%', '%%'))
        print('** Running upgrade')
        command.upgrade(config, 'e4bbb5a200bb')
        print('** Upgrade completed')


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
        collect_dags(current_app)
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
    collect_dags(current_app)
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

    task_log_reader = TaskLogReader()
    log_data = []
    for ti in last_run.get_task_instances():
        if ti.start_date is not None:
            log_data.append({
                "task_id": ti.task_id,
                "logs": task_log_reader.read_log_chunks(
                    ti,
                    try_number=ti.try_number,
                    metadata={},
                )[0][0]
            })
    return jsonify(log_data)
