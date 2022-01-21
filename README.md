# Airflow Derived DAG API

Plugin allowing for the creation of DAGs dynamically via calls to the API. 

## To install

1. Ensure you have a plugin directory configured for airflow (`AIRFLOW__CORE__PLUGINS_FOLDER=/path/to/airflow/plugins`)
2. Clone this project into the plugin directory`git clone git@github.com/uktrade/derived-dag-api.git /path/to/airflow/plugins/derived-dag-api`
3. Copy the file '~dags/derived_dag_api_dags.py' into your airflow dags directory (as set by `AIRFLOW__CORE__DAGS_FOLDER`) 
## To use

#### Create an SQL based derived DAG
```bash
curl --request POST \
  --url "https://<your-airflow-domain>/api/experimental/derived-dags/dag/<new dag id>" 
  --header "Content-Type: application/json" \
  --data '{
    "schedule": "@daily",
    "config": {"sql": "select 1, 2, 3"},
    "enabled": true,
    "type": "sql",
    "table_name": "<output table name>",
    "schema_name": "<output schema name>"
  }'
```

#### Update an existing derived DAG
```bash
curl --request PUT \
  --url "https://<your-airflow-domain>/api/experimental/derived-dags/dag/<existing dag id>" 
  --header "Content-Type: application/json" \
  --data '{
    "schedule": "@daily",
    "config": {"sql": "select 1, 2, 3"},
    "enabled": true,
  }'
```

#### Delete a derived DAG
```bash
curl --request DELETE \
  --url "https://<your-airflow-domain>/api/experimental/derived-dags/dag/<existing dag id>" 
```


#### Fetch logs for the latest run of a derived DAG
```bash
curl --request DELETE \
  --url "https://<your-airflow-domain>/api/experimental/derived-dags/dag/<existing dag id>/logs" 
```
