from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import subprocess

# Helper to execute each Python script
def run_script(script_name):
    script_path = os.path.join(os.path.dirname(__file__), "etl", script_name)
    subprocess.run(["python", script_path], check=True)

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# Define the DAG
with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    description="Run Extract, Transform, and Load scripts in sequence",
    schedule="@daily",  # ⏰ runs every 5 minutes
    start_date=datetime(2025, 11, 4),
    catchup=True,
    tags=["ETL", "example"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=run_script,
        op_args=["extract.py"],
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=run_script,
        op_args=["transform.py"],
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=run_script,
        op_args=["load.py"],
    )

    # Define dependencies: Extract → Transform → Load
    extract_task >> transform_task >> load_task