import json
import logging
import os
import requests
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict

# ---- Setup logger ----
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def extract_from_api(url: str, headers: Optional[Dict[str, str]] = None, base_dir: str = "/opt/airflow/data") -> str:
    """
    Extract data from an API and save it as a JSON file.

    Args:
        url: API endpoint to request data from.
        headers: Optional HTTP headers.
        base_dir: Directory to save extracted JSON files.

    Returns:
        Path to the saved JSON file.
    """
    # ---- 1. Make API Request ----
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        response_json = response.json()
    except requests.RequestException as e:
        logger.error("API request failed: %s", e)
        raise

    # ---- 2. Ensure base directory exists ----
    Path(base_dir).mkdir(parents=True, exist_ok=True)

    # --- 3. Generate timestamped file path ----
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_path = Path(base_dir) / f"extract_{timestamp}.json"

    # ---- 4. Save JSON to file ----
    with open(file_path, "w") as f:
        json.dump(response_json, f, indent=4)

    logger.info("Extract saved → %s", file_path)
    return str(file_path)
