"""
ETL Pipeline DAG for Stock Market Data

This DAG orchestrates an ETL (Extract, Transform, Load) pipeline for stock market data
using Apache Airflow. It integrates with Alpha Vantage for data extraction, processes
the data in Python, loads it into Google BigQuery, and sends a Slack notification upon success.

Modules:
- extract_from_api: Pulls stock market data from Alpha Vantage API
- transform_market_data: Cleans and normalizes the extracted data
- load_to_bigquery: Loads processed data into BigQuery
- get_config: Loads configuration from Airflow Variables or .env
- notify_slack_success: Sends Slack notifications after successful DAG run
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from src.stock_etl_pipeline.etl.extract import extract_from_api
from src.stock_etl_pipeline.etl.transform import transform_market_data
from src.stock_etl_pipeline.etl.load import load_to_bigquery
from src.stock_etl_pipeline.utils.config import get_config
from src.stock_etl_pipeline.utils.slack import notify_slack_success

# ───────────────────────────────
# Default arguments for the DAG
# ───────────────────────────────
default_args = {
    "owner": "Matias-Gays",           # Responsible owner for DAG
    "depends_on_past": False,          # Do not wait for previous DAG runs
    "retries": 1,                      # Retry once on failure
    "execution_timeout": timedelta(seconds=300),  # Max execution time per task
}

# ───────────────────────────────
# Load configuration
# ───────────────────────────────
config = get_config()  # Loads config from Airflow Variables or .env

API_CONFIG = config["alpha_vantage"]
BQ_CONFIG = config["bigquery"]
SLACK_CONFIG = config["slack"]
GCP_CREDENTIALS = config["gcp_credentials"]

# Construct the Alpha Vantage API endpoint
API_ENDPOINT = (
    f"{API_CONFIG['url']}?function={API_CONFIG['function']}"
    f"&symbol={API_CONFIG['symbol']}"
    f"&interval={API_CONFIG['interval']}"
    f"&apikey={API_CONFIG['api_key']}"
)

# ───────────────────────────────
# Define the DAG
# ───────────────────────────────
with DAG(
    dag_id="etl_pipeline",                     # Unique DAG ID
    default_args=default_args,                 # Default task arguments
    description="Run Extract, Transform, and Load scripts in sequence",
    schedule="@daily",                         # DAG schedule interval
    start_date=datetime(2025, 11, 14),        # Start date for DAG runs
    catchup=False,                             # Do not backfill missing runs
    tags=["ETL"],                              # Tags for categorization in UI
    max_active_runs=1,                         # Prevent parallel runs
) as dag:

    # ───────────────────────────────
    # Task 1: Extract
    # ───────────────────────────────
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_from_api,       # Calls extract function
        op_args=[API_ENDPOINT],                 # Pass API endpoint
    )

    # ───────────────────────────────
    # Task 2: Transform
    # ───────────────────────────────
    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_market_data,  # Calls transform function
        op_args=["{{ ti.xcom_pull('extract') }}"],  # Pull data from previous task
    )

    # ───────────────────────────────
    # Task 3: Load
    # ───────────────────────────────
    load_task = PythonOperator(
        task_id="load",
        python_callable=load_to_bigquery,       # Calls load function
        op_args=[
            "{{ ti.xcom_pull('transform') }}",  # Pull transformed data
            GCP_CREDENTIALS,                    # GCP Service Account
            BQ_CONFIG                            # BigQuery config
        ],
        op_kwargs={"if_exists": "replace"},      # Overwrite table if exists
    )

    # ───────────────────────────────
    # Task 4: Slack Notification
    # ───────────────────────────────
    success_task = PythonOperator(
        task_id="success_notification",
        python_callable=notify_slack_success,   # Send Slack notification
        op_args=[
            "{{ ti.xcom_pull('load') }}",       # Pull result from load task
            SLACK_CONFIG["webhook_url"]         # Slack webhook URL
        ]
    )

    # ───────────────────────────────
    # Define Task Dependencies
    # ───────────────────────────────
    extract_task >> transform_task >> load_task >> success_task
