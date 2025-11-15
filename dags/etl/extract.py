import json
import requests
import os
from datetime import datetime, timezone

def extract_from_api(url: str, headers: dict = None) -> str:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    response_json = response.json()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base_dir = "/opt/airflow/data"
    os.makedirs(base_dir, exist_ok=True)
    
    file_path = os.path.join(base_dir, f"extract_{timestamp}.json")

    with open(file_path, "w") as f:
        json.dump(response_json, f, indent=4)

    print(f"Extract saved → {file_path}")
    return file_path
