"""
Slack Notification Helper for Stock ETL Pipeline

This module provides a function to send success notifications
to a Slack channel via an Incoming Webhook.

Modules/Functions:
- notify_slack_success(): Sends a message when the DAG finishes successfully
"""

import json
from typing import Union, Dict, Any
import requests
import logging
from airflow.models import Variable

# Initialize logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def notify_slack_success(
    context: bool,
    webhook_url: str
) -> bool:
    """
    Sends a Slack message when the DAG finishes successfully.

    Args:
        context: Boolean indicating if the DAG completed successfully.
        webhook_url: Slack Incoming Webhook URL.

    Returns:
        True if the message was sent successfully, False otherwise.

    Behavior:
        - If webhook_url is missing, logs a message and returns False.
        - Sends a JSON payload with a success message to Slack.
        - Logs success or failure of the request.
    """

    if not webhook_url:
        logger.info("No Slack webhook URL provided; skipping notification.")
        return False

    message = {
        "text": "✅ DAG completed successfully!"
    }

    if context:
        try:
            requests.post(webhook_url, json=message, timeout=10)
            logger.info("DAG sent Slack notification successfully.")
            return True
        except Exception as e:
            logger.error(
                "DAG completed successfully but failed to send Slack notification: %s", e
            )
            return False

    logger.info("DAG did not complete successfully; no Slack notification sent.")
    return False
