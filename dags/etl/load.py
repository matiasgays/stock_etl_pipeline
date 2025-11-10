import os
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_to_bigquery(
    df_json_str: str,
    project_id: str,
    dataset_name: str = "sales_dataset",
    table_name: str = "sales",
    location: str = "US",
    credentials_env_var: str = "GOOGLE_APPLICATION_CREDENTIALS",
    timeout: int = 300,
) -> int:
    """
    Load a JSON records string into BigQuery. Returns number of rows loaded.
    Raises for missing credentials, empty input, or load failures.
    """

    if not df_json_str:
        raise ValueError("Received empty data from XCom")

    # decode JSON into DataFrame (expects records orient)
    df = pd.read_json(df_json_str, orient="records")
    # Sort the dataframe by timestamp before loading
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values(by="timestamp", ascending=True)

    # Resolve credentials path: allow absolute env var or relative to project root
    base_dir = Path(__file__).resolve().parents[2]  # go up from dags/etl/ to project root
    cred_env_val = os.getenv(credentials_env_var)
    if not cred_env_val:
        raise EnvironmentError(f"{credentials_env_var} is not set")

    cred_path = Path(cred_env_val)
    if not cred_path.is_absolute():
        cred_path = base_dir.joinpath(cred_path)

    if not cred_path.exists():
        raise FileNotFoundError(f"Service account file not found at: {cred_path}")

    logger.info("Using service account file: %s", cred_path)

    credentials = service_account.Credentials.from_service_account_file(str(cred_path))
    client = bigquery.Client(credentials=credentials, project=project_id)

    dataset_id = f"{project_id}.{dataset_name}"
    table_id = f"{dataset_id}.{table_name}"

    # Ensure dataset exists
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = location
    client.create_dataset(dataset, exists_ok=True)
    logger.info("Dataset ensured: %s (location=%s)", dataset_id, location)

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    # Load dataframe (for very large data consider chunking or using GCS staging)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result(timeout=timeout)
    if job.errors:
        raise RuntimeError(f"Load job failed: {job.errors}")

    loaded_rows = int(job.output_rows or 0)
    logger.info("Loaded %d rows into %s", loaded_rows, table_id)

    # sample verification (only limited rows)
    sample_query = "SELECT * FROM `productos-320620.sales_dataset.sales`"
    df_sample = client.query(sample_query).to_dataframe()
    logger.info("Sample rows:\n%s", df_sample.head())

    return loaded_rows


def load():
    logger.info("Loading data... (no-op helper)")
    return
