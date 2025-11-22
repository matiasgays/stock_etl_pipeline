import os
from airflow.models import Variable

def get_config():
    """
    Carga la configuración del pipeline utilizando:
    1. Variables de Airflow (prioridad máxima)
    2. Variables de entorno desde `.env` (fallback)
    """

    config = {
        # ───────────────────────────────────────────────────────
        # Stock API (Alpha Vantage)
        # ───────────────────────────────────────────────────────
        "alpha_vantage": {
            "api_key": Variable.get(
                "alpha_vantage_api_key",
                default_var=os.getenv("DEFAULT_ALPHA_VANTAGE_KEY")
            ),
            "symbol": Variable.get(
                "stock_symbol",
                default_var=os.getenv("DEFAULT_STOCK_SYMBOL")
            ),
            "interval": Variable.get(
                "alpha_vantage_interval",
                default_var=os.getenv("DEFAULT_ALPHA_VANTAGE_INTERVAL")
            ),
            "function": Variable.get(
                "alpha_vantage_function",
                default_var=os.getenv("DEFAULT_ALPHA_VANTAGE_FUNCTION")
            ),
            "url": Variable.get(
                "alpha_vantage_url",
                default_var=os.getenv("DEFAULT_ALPHA_VANTAGE_URL")
            ),
        },

        # ───────────────────────────────────────────────────────
        # BigQuery Config
        # ───────────────────────────────────────────────────────
        "bigquery": {
            "dataset": Variable.get(
                "bigquery_dataset",
                default_var=os.getenv("DEFAULT_DATASET_NAME")
            ),
            "table": Variable.get(
                "bigquery_table",
                default_var=os.getenv("DEFAULT_TABLE_NAME")
            ),
            "location": Variable.get(
                "bigquery_location",
                default_var=os.getenv("DEFAULT_DATASET_LOCATION")
            ),
        },

        # ───────────────────────────────────────────────────────
        # Slack Notifications (opcional)
        # ───────────────────────────────────────────────────────
        "slack": {
            "webhook_url": Variable.get(
                "slack_webhook_url",
                default_var=os.getenv("DEFAULT_SLACK_WEBHOOK_URL", None)
            )
        }
    }

    _validate_config(config)
    return config


def _validate_config(config: dict):
    """
    Valida que las configuraciones esenciales estén presentes.
    Evita fallas silenciosas.
    """

    required = [
        ("alpha_vantage.api_key", config["alpha_vantage"]["api_key"]),
        ("alpha_vantage.symbol", config["alpha_vantage"]["symbol"]),
        ("bigquery.dataset", config["bigquery"]["dataset"]),
        ("bigquery.table", config["bigquery"]["table"]),
    ]

    missing = [key for key, value in required if not value]

    if missing:
        raise ValueError(
            f"Missing required configuration values: {missing}. "
            "Set them as Airflow Variables or in your .env file."
        )
