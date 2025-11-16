from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

from etl.extract import extract_from_api
from etl.transform import transform_market_data
from etl.load import load_to_bigquery
from airflow.models import Variable
from datetime import timedelta

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "execution_timeout": timedelta(seconds=300),
}

API_CONFIG = Variable.get("API_CONFIG", deserialize_json=True)
GCP_SERVICE_ACCOUNT = Variable.get("GCP_SERVICE_ACCOUNT", deserialize_json=True)
GCP_BIGQUERY_CONFIG = Variable.get("GCP_BIGQUERY_CONFIG", deserialize_json=True)

API_ENDPOINT = (
    f"{API_CONFIG['url']}?function={API_CONFIG['function']}"
    f"&symbol={API_CONFIG['symbol']}"
    f"&interval={API_CONFIG['interval']}"
    f"&apikey={API_CONFIG['key']}"
)

# Define the DAG
with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    description="Run Extract, Transform, and Load scripts in sequence",
    schedule="@daily",
    start_date=datetime(2025, 11, 13),
    catchup=False,
    tags=["ETL", "example"],
    max_active_runs=1,  # Prevent parallel runs
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_from_api,
        op_args=[API_ENDPOINT],
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_market_data,
        op_args=["{{ ti.xcom_pull('extract') }}"],
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load_to_bigquery,
        op_args=[
            "{{ ti.xcom_pull('transform') }}",
            GCP_SERVICE_ACCOUNT,
            GCP_BIGQUERY_CONFIG
        ],
        op_kwargs={"if_exists": "replace"},
    )

    extract_task >> transform_task >> load_task
