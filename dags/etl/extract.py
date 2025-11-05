import requests
import pandas as pd
import json

def extract():
    print("Extracting data...")
    return

def extract_from_api(url: str, headers: dict = None) -> dict:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    with open("/home/mello/stock/src/stock/extract/data/response.json", "w") as f:
        json.dump(data, f, indent=4)
    return data