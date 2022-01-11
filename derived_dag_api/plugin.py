import os

from airflow.plugins_manager import AirflowPlugin
from airflow.settings import SQL_ALCHEMY_CONN
from airflow.www.api.experimental.endpoints import (
    api_experimental,
    requires_authentication,
)
from alembic import command
from flask import jsonify


class DerivedDagApiPlugin(AirflowPlugin):
    name = "derived_dag_api"
    flask_blueprints = [api_experimental]

    def on_load(*args, **kwargs):
        from alembic.config import Config

        print('Running migrations for derived dag api')
        current_dir = os.path.dirname(os.path.abspath(__file__))
        directory = os.path.join(current_dir, 'alembic')
        config = Config(os.path.join(current_dir, 'alembic.ini'))
        config.set_main_option('script_location', directory.replace('%', '%%'))
        config.set_main_option('sqlalchemy.url', SQL_ALCHEMY_CONN.replace('%', '%%'))
        command.upgrade(config, 'heads')


@api_experimental.route('/derived-dags/test', methods=['GET'])
@requires_authentication
def derived_dags_test():
    return jsonify(status="Derived dags plugin loaded OK")


@api_experimental.route('/derived-dags/dag/<string:dag_id>', methods=['POST', 'DELETE'])
@requires_authentication
def derived_dags_dag(dag_id):
    return jsonify(status=f"TODO: CREATE/UPDATE/DELETE for DAG {dag_id}")


@api_experimental.route('/derived-dags/dag/<string:dag_id>/logs', methods=['GET'])
@requires_authentication
def derived_dags_dag_log(dag_id):
    return jsonify(status=f"TODO: Return combined task logs for DAG {dag_id}")
