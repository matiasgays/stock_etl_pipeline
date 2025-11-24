"""
Configuration Loader for Stock ETL Pipeline

This module centralizes configuration management for the ETL pipeline.
It supports:

1. Reading Airflow Variables
2. Falling back to Environment Variables (.env)
3. Using default values if neither is set

Additionally, it handles GCP Service Account credentials:
- Load from a JSON file path (local or mounted)
- Load from an embedded JSON string in Airflow Variables
- Normalize and validate the private key

Modules/Functions:
- get_config(): main entry point for loading all pipeline configuration
- _get(): helper for reading Airflow Variable > env > default
- _load_gcp_credentials(): loads and validates GCP credentials
- _normalize_private_key(): fixes line breaks in PEM keys
- _validate_gcp_credentials(): checks required fields and format
"""

import os
import json
from airflow.models import Variable

# ───────────────────────────────
# Helper: Read variable
# ───────────────────────────────
def _get(var_name: str, env_name: str, default=None):
    """
    Return the configuration value in this priority:
    1) Airflow Variable
    2) Environment variable
    3) Default value
    """
    return Variable.get(var_name, default_var=os.getenv(env_name, default))


# ───────────────────────────────
# Helper: Load & normalize GCP Service Account JSON
# ───────────────────────────────
def _load_gcp_credentials(gcp_value: str) -> dict:
    """
    Accepts either:
    - A file path to a service account JSON
    - A raw JSON string stored in Airflow

    Returns a validated credentials dictionary.
    """

    if not gcp_value:
        raise ValueError(
            "Missing GCP credentials. Set Airflow Variable 'gcp_credentials' "
            "or environment variable GCP_CREDENTIALS with a file path or JSON string."
        )

    # 1) Try embedded JSON in Airflow Variable
    embedded_json = Variable.get("gcp_credentials_json", default_var=None)
    if embedded_json:
        creds = json.loads(embedded_json)
        return _validate_gcp_credentials(creds)

    # 2) Fallback: path to JSON file in .env
    path = os.getenv("GCP_CREDENTIALS")
    if path:
        if not os.path.exists(path):
            raise FileNotFoundError(f"GCP credentials file not found at path: {path}")

        with open(path, "r") as f:
            creds = json.load(f)
            _normalize_private_key(creds)
        return _validate_gcp_credentials(creds)

    raise ValueError(
        "No GCP credentials found. "
        "Add the JSON file in Airflow ('gcp_credentials_json') "
        "or locally at 'dags/resources/gcp_credentials.json', "
        "or set GCP_CREDENTIALS in .env"
    )


# ───────────────────────────────
# Main configuration loader
# ───────────────────────────────
def get_config():
    """
    Loads configuration for the ETL pipeline from:
    1) Airflow Variables
    2) Environment variables (.env)
    3) Defaults

    Returns a dictionary containing:
    - Alpha Vantage API config
    - BigQuery config
    - GCP credentials
    - Slack webhook URL
    """

    config = {
        "alpha_vantage": {
            "api_key": _get("alpha_vantage_api_key", "ALPHA_VANTAGE_KEY", "demo"),
            "symbol": _get("alpha_vantage_stock_symbol", "ALPHA_VANTAGE_STOCK_SYMBOL", "IBM"),
            "interval": _get("alpha_vantage_interval", "ALPHA_VANTAGE_INTERVAL", "5min"),
            "function": _get("alpha_vantage_function", "ALPHA_VANTAGE_FUNCTION", "TIME_SERIES_INTRADAY"),
            "url": _get("alpha_vantage_url", "ALPHA_VANTAGE_URL", "https://www.alphavantage.co/query"),
        },
        "bigquery": {
            "dataset": _get("bigquery_dataset", "DATASET_NAME", "default_dataset"),
            "table": _get("bigquery_table", "TABLE_NAME", "default_table"),
            "location": _get("bigquery_location", "DATASET_LOCATION", "US"),
        },
        "gcp_credentials": _load_gcp_credentials(
            _get("gcp_credentials", "GCP_CREDENTIALS", None)
        ),
        "slack": {
            "webhook_url": _get("slack_webhook_url", "SLACK_WEBHOOK_URL", None)
        },
    }

    return config


# ───────────────────────────────
# Helper: Normalize PEM private key
# ───────────────────────────────
def _normalize_private_key(creds: dict):
    """
    Fixes line breaks in the private_key field to prevent auth errors.
    """
    if "private_key" in creds and "\\n" in creds["private_key"]:
        creds["private_key"] = creds["private_key"].replace("\\n", "\n")
    return creds


# ───────────────────────────────
# Helper: Validate GCP Service Account
# ───────────────────────────────
def _validate_gcp_credentials(creds: dict) -> dict:
    """
    Validates that the GCP Service Account JSON includes:
    - All required keys
    - Non-empty values
    - Proper PEM private_key format

    Raises ValueError if invalid.
    """

    required_keys = {
        "type",
        "project_id",
        "private_key_id",
        "private_key",
        "client_email",
        "client_id",
        "auth_uri",
        "token_uri",
        "auth_provider_x509_cert_url",
        "client_x509_cert_url",
    }

    # Check missing fields
    missing = [key for key in required_keys if key not in creds]
    if missing:
        raise ValueError(f"GCP credentials missing required fields: {missing}")

    # Check empty values
    empty = [key for key in required_keys if not creds.get(key)]
    if empty:
        raise ValueError(f"GCP credentials contain empty values: {empty}")

    # Validate PEM format for private_key
    pk = creds["private_key"]
    if not pk.startswith("-----BEGIN PRIVATE KEY-----"):
        raise ValueError("GCP private_key is not a valid PEM key (missing BEGIN header).")
    if not pk.strip().endswith("-----END PRIVATE KEY-----"):
        raise ValueError("GCP private_key is not a valid PEM key (missing END footer).")

    return creds
