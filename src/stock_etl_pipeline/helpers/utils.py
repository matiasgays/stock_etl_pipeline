import json
from typing import Union, Dict, Any
import requests
from airflow.models import Variable

def load_gcp_credentials(gcp_service_account: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Load and normalize a GCP service account.
    If input is a file path, it loads and parses the JSON.
    It also fixes escaped newlines in the private_key.
    """

    # If the input is a string, assume it's a file path
    if isinstance(gcp_service_account, str):
        with open(gcp_service_account, "r") as f:
            gcp_service_account = json.load(f)

    # Fix escaped newline characters in private_key
    if "private_key" in gcp_service_account and "\\n" in gcp_service_account["private_key"]:
        gcp_service_account["private_key"] = gcp_service_account["private_key"].replace("\\n", "\n")

    return gcp_service_account

def notify_slack_success(context):
    """
    Sends a Slack message when the DAG finishes successfully.
    """
    webhook_url = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
    print("Slack Webhook URL:", webhook_url)

    if not webhook_url:
        print("⚠ No Slack webhook configured in Airflow Variables.")
        return

    """dag_id = context["dag"].dag_id
    run_id = context["run_id"]"""

    message = {
        "text": f"✅ DAG  completed successfully!\nRun ID: run_id"
    }

    try:
        requests.post(webhook_url, json=message, timeout=10)
    except Exception as e:
        print(f"Slack notification failed: {e}")
