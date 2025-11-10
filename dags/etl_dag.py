from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import subprocess

# Helper to execute each Python script
def run_script(script_name):
    script_path = os.path.join(os.path.dirname(__file__), "etl", script_name)
    subprocess.run(["python", script_path], check=True)

from etl.extract import extract_from_api
from etl.transform import transform_market_data
from etl.load import load_to_bigquery

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

project_id = "productos-320620"

# Define the DAG
with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    description="Run Extract, Transform, and Load scripts in sequence",
    schedule="@daily",  # ⏰ runs every 5 minutes
    start_date=datetime(2025, 11, 6),
    catchup=False,
    tags=["ETL", "example"],
) as dag:

    # Example of arguments to pass
    api_url = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=demo" 

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_from_api,  # direct function call
        op_args=[api_url],
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_market_data,
        op_args=["{{ ti.xcom_pull(task_ids='extract') }}"],
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load_to_bigquery,
        op_args=[
            "{{ ti.xcom_pull(task_ids='transform') }}",  # XCom pulled → rendered as str
            project_id,
        ],
        op_kwargs={"if_exists": "replace"},
    )

    # Define dependencies: Extract → Transform → Load
    extract_task >> transform_task >> load_task