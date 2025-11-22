from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import json

from src.stock_etl_pipeline.etl.extract import extract_from_api
from src.stock_etl_pipeline.etl.transform import transform_market_data
from src.stock_etl_pipeline.etl.load import load_to_bigquery
from src.stock_etl_pipeline.helpers.config import get_config
from src.stock_etl_pipeline.helpers.utils import notify_slack_success
from airflow.models import Variable

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "execution_timeout": timedelta(seconds=300),
}

config = get_config()

API_CONFIG = config["alpha_vantage"]
BQ_CONFIG = config["bigquery"]
SLACK_CONFIG = config["slack"]
GCP_CREDENTIALS = os.getenv("GCP_CREDENTIALS")

API_ENDPOINT = (
    f"{API_CONFIG['url']}?function={API_CONFIG['function']}"
    f"&symbol={API_CONFIG['symbol']}"
    f"&interval={API_CONFIG['interval']}"
    f"&apikey={API_CONFIG['api_key']}"
)

# Define the DAG
with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    description="Run Extract, Transform, and Load scripts in sequence",
    schedule="@daily",
    start_date=datetime(2025, 11, 14),
    catchup=False,
    tags=["ETL"],
    max_active_runs=1,  # Prevent parallel runs
) as dag:
    success_task = PythonOperator(
        task_id="check_callback_trigger",
        python_callable=notify_slack_success,
        op_args=[SLACK_CONFIG["webhook_url"]],
    )

    """extract_task = PythonOperator(
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
            GCP_CREDENTIALS,
            BQ_CONFIG
        ],
        op_kwargs={"if_exists": "replace"},
    )"""

    # extract_task >> transform_task >> load_task
    success_task
