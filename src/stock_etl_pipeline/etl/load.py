"""
BigQuery Loader for Stock ETL Pipeline

This module provides a function to load JSON or transformed DataFrame data
into Google BigQuery using a validated GCP Service Account.

Modules/Functions:
- load_to_bigquery(): Load a JSON file into a specified BigQuery table
"""

import json
import logging
from typing import Optional

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# Initialize logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_to_bigquery(
    file_path: str,
    gcp_service_account: dict,
    gcp_bigquery_config: dict,
    timeout: Optional[int] = 300,
) -> bool:
    """
    Load a JSON file into BigQuery.

    Steps:
    1. Load GCP credentials from the service account dict
    2. Validate BigQuery config: dataset, table, location
    3. Read JSON into a Pandas DataFrame
    4. Normalize the 'timestamp' column (if present)
    5. Ensure the dataset exists
    6. Load DataFrame into BigQuery

    Args:
        file_path: Path to the JSON file containing stock data.
        gcp_service_account: Dictionary with GCP Service Account credentials.
        gcp_bigquery_config: Dictionary with keys 'dataset', 'table', and optional 'location'.
        timeout: Job timeout in seconds (default: 300).

    Returns:
        True if the load succeeded, False otherwise.

    Raises:
        ValueError if required GCP credentials or BigQuery config is missing.
    """

    # ---- 1. Credentials ----
    credentials = service_account.Credentials.from_service_account_info(gcp_service_account)
    
    project = gcp_service_account.get("project_id")
    if not project:
        raise ValueError("Service account must include 'project_id'.")

    # ---- 2. BigQuery Config ----
    dataset = gcp_bigquery_config.get("dataset")
    table = gcp_bigquery_config.get("table")
    location = gcp_bigquery_config.get("location")

    if not dataset or not table:
        raise ValueError("BigQuery config must include 'dataset' and 'table'.")

    dataset_id = f"{project}.{dataset}"
    table_id = f"{dataset_id}.{table}"

    logger.info("Loading data → File: %s", file_path)
    logger.info("Target BigQuery: %s", table_id)

    # ---- 3. Load JSON into DataFrame ----
    with open(file_path, "r") as f:
        records = json.load(f)

    df = pd.DataFrame(records)
    
    # ---- 3a. Normalize timestamp ----
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    else:
        logger.warning("No 'timestamp' column found in input. Skipping timestamp processing.")

    # ---- 4. Initialize BigQuery client ----
    client = bigquery.Client(credentials=credentials, project=project)

    # ---- 5. Ensure dataset exists ----
    dataset_obj = bigquery.Dataset(dataset_id)
    dataset_obj.location = location
    client.create_dataset(dataset_obj, exists_ok=True)
    logger.info("Dataset ensured: %s (location=%s)", dataset_id, location)

    # ---- 6. Load DataFrame to BigQuery ----
    try:
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result(timeout=timeout)
        loaded_rows = job.output_rows or 0
        logger.info("Successfully loaded %d rows into %s", loaded_rows, table_id)
        return True
    except Exception as e:
        logger.error("BigQuery Load errors: %s", getattr(job, "errors", e))
        return False
