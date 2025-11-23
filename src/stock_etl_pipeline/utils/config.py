import os
import json
from airflow.models import Variable


# ------------------------------------------------------------
# Helper: Read variable (Airflow > env > default)
# ------------------------------------------------------------
def _get(var_name: str, env_name: str, default=None):
    return Variable.get(var_name, default_var=os.getenv(env_name, default))


# ------------------------------------------------------------
# Helper: Load & normalize Service Account JSON
# ------------------------------------------------------------
def _load_gcp_credentials(gcp_value: str) -> dict:
    """
    Accepts either:
    - A file path to a service account JSON
    - A raw JSON string stored in Airflow or .env

    Returns a valid normalized credentials dict.
    """
    if not gcp_value:
        raise ValueError(
            "Missing GCP credentials. Set Airflow Variable 'gcp_credentials' "
            "or environment variable GCP_CREDENTIALS with a file path or JSON string."
        )

    # 1️⃣ Intentar JSON embebido en Airflow Variable
    embedded_json = Variable.get("gcp_credentials_json", default_var=None)
    if embedded_json:
        creds = json.loads(embedded_json)
        return _validate_gcp_credentials(creds)

    # 2️⃣ Case 2 → path to the JSON file in .env
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
        "Add the JSON file in Airflow: Set the Airflow Variable 'gcp_credentials_json' or locally in the project: 'dags/resources/gcp_credentials.json', "
        "or checked that GCP_CREDENTIALS is defined in .env"
    )


# ------------------------------------------------------------
# Main configuration loader
# ------------------------------------------------------------
def get_config():
    """
    Loads configuration from:
    1) Airflow Variables
    2) Environment variables
    """

    config = {
        # ───────────────────────────────────────────────────
        # Stock API
        # ───────────────────────────────────────────────────
        "alpha_vantage": {
            "api_key": _get("alpha_vantage_api_key", "ALPHA_VANTAGE_KEY", "demo"),
            "symbol": _get("alpha_vantage_stock_symbol", "ALPHA_VANTAGE_STOCK_SYMBOL", "IBM"),
            "interval": _get("alpha_vantage_interval", "ALPHA_VANTAGE_INTERVAL", "5min"),
            "function": _get("alpha_vantage_function", "ALPHA_VANTAGE_FUNCTION", "TIME_SERIES_INTRADAY"),
            "url": _get("alpha_vantage_url", "ALPHA_VANTAGE_URL", "https://www.alphavantage.co/query"),
        },

        # ───────────────────────────────────────────────────
        # BigQuery Config
        # ───────────────────────────────────────────────────
        "bigquery": {
            "dataset": _get("bigquery_dataset", "DATASET_NAME", "default_dataset"),
            "table": _get("bigquery_table", "TABLE_NAME", "default_table"),
            "location": _get("bigquery_location", "DATASET_LOCATION", "US"),
        },

        # ───────────────────────────────────────────────────
        # GCP Credentials
        # ───────────────────────────────────────────────────
        "gcp_credentials": _load_gcp_credentials(
            _get("gcp_credentials", "GCP_CREDENTIALS", None)
        ),

        # ───────────────────────────────────────────────────
        # Slack
        # ───────────────────────────────────────────────────
        "slack": {
            "webhook_url": _get("slack_webhook_url", "SLACK_WEBHOOK_URL", None)
        },
    }

    return config


# ------------------------------------------------------------
# 
# -----------------------------------------------------------
def _normalize_private_key(creds: dict):
    """
    Reemplaza '\\n' por '\n' en private_key.
    Evita errores de autenticación.
    """
    if "private_key" in creds and "\\n" in creds["private_key"]:
        creds["private_key"] = creds["private_key"].replace("\\n", "\n")
    return creds

def _validate_gcp_credentials(creds: dict) -> dict:
    """
    Validates that the GCP Service Account JSON includes:
    - All required keys
    - Non-empty values
    - Proper private_key formatting

    Raises ValueError on any invalid condition.
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

    # 1) Check presence of required fields
    missing = [key for key in required_keys if key not in creds]
    if missing:
        raise ValueError(f"GCP credentials missing required fields: {missing}")

    # 2) Check empty values
    empty = [key for key in required_keys if not creds.get(key)]
    if empty:
        raise ValueError(f"GCP credentials contain empty values: {empty}")

    # 3) Validate private_key format
    pk = creds["private_key"]

    if not pk.startswith("-----BEGIN PRIVATE KEY-----"):
        raise ValueError("GCP private_key is not a valid PEM key (missing BEGIN header).")

    if not pk.strip().endswith("-----END PRIVATE KEY-----"):
        raise ValueError("GCP private_key is not a valid PEM key (missing END footer).")

    return creds