import json
from typing import Union, Dict, Any
import requests
import logging
from airflow.models import Variable

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def notify_slack_success(
        context: bool,
        webhook_url: str
        ) -> bool:
    """
    Sends a Slack message when the DAG finishes successfully.
    """

    if not webhook_url:
        logger.info("No Slack webhook URL provided; skipping notification.")
        return False

    message = {
        "text": f"✅ DAG completed successfully!"
    }

    if context:
        try:
            requests.post(webhook_url, json=message, timeout=10)
            logger.info("DAG sent Slack notification successfully.")
            return True
        except Exception as e:
            logger.error("DAG completed successfully but failed to send Slack notification: %s", e)
            return False
    
    logger.info("DAG did not complete successfully; no Slack notification sent.")
    return False