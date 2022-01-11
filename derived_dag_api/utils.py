import os
from http.client import BAD_REQUEST
from urllib.parse import urlparse

from flask import jsonify, make_response


def get_database_uri():
    parsed_uri = urlparse(os.environ['AIRFLOW__CORE__SQL_ALCHEMY_CONN'])
    host, port, dbname, user, password = (
        parsed_uri.hostname,
        parsed_uri.port or 5432,
        parsed_uri.path.strip('/'),
        parsed_uri.username,
        parsed_uri.password,
    )
    return f'host={host} port={port} dbname={dbname} user={user} password={password}'


def bad_request_response(message, status=BAD_REQUEST):
    return make_response(jsonify(status=message), status)


def collect_dags(current_app):
    derived_dag_file = os.path.join(current_app.dag_bag.dag_folder, 'derived_dag_api_dags.py')
    current_app.dag_bag.collect_dags(
        dag_folder=(
            derived_dag_file
            if os.path.exists(derived_dag_file)
            else current_app.dag_bag.dag_folder
        ),
        include_examples=False,
        include_smart_sensor=False,
        safe_mode=False,
    )
