import json
import logging
from typing import Optional

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_to_bigquery(
    file_path: str,
    gcp_service_account: dict,
    gcp_bigquery_config: dict,
    timeout: Optional[int] = 300,
) -> int:
    """
    Load a JSON file into BigQuery.

    Args:
        file_path: Path to the JSON file.
        gcp_service_account: Dict with service account credentials.
        gcp_bigquery_config: Dict with 'dataset', 'table', and optional 'location'.
        timeout: Job timeout in seconds.

    Returns:
        Number of rows loaded.
    """

    # ---- 1. Credentials ----
    credentials = service_account.Credentials.from_service_account_info(
        gcp_service_account
    )
    project = gcp_service_account.get("project_id")
    if not project:
        raise ValueError("Service account must include 'project_id'.")

    # ---- 2. BigQuery Config ----
    dataset = gcp_bigquery_config.get("dataset")
    table = gcp_bigquery_config.get("table")
    location = gcp_bigquery_config.get("location", "US")

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
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result(timeout=timeout)

    if job.errors:
        logger.error("BigQuery Load errors: %s", job.errors)
        raise RuntimeError(f"Load job failed: {job.errors}")

    loaded_rows = job.output_rows or 0
    logger.info("Successfully loaded %d rows into %s", loaded_rows, table_id)

    return int(loaded_rows)
