import os
from http.client import BAD_REQUEST
from urllib.parse import urlparse

from airflow.models import DagBag
from flask import jsonify, make_response

from .models import DerivedPipelines


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


def collect_dags():
    print("Collecting and syncing dags manually")
    dag_bag = DagBag()
    dag_bag.sync_to_db()
    print("Finished collecting and syncing dags")


def derived_dag_exists(session, dag_id):
    return session.query(DerivedPipelines).filter(DerivedPipelines.dag_id == dag_id).first() is not None