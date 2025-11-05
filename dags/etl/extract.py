import os
import json
import requests

def extract_from_api(url: str, headers: dict = None) -> dict:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    return json.dumps(data)


def extract():
    # Define dynamic output path
    output_dir = os.path.dirname(__file__)
    output_path = os.path.join(output_dir, "response.json")

    # Ensure the folder exists
    os.makedirs(output_dir, exist_ok=True)

    # Save DataFrame to JSON
    #with open(output_path, "w") as f:
    #    json.dump(data, f, indent=4)

    print(f"✅ Data saved to: {output_path}")
    print("Extracting data...")
    return