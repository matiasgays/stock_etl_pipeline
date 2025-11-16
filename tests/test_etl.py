import json
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

from dags.etl.extract import extract_from_api
from dags.etl.transform import transform_market_data
from dags.etl.load import load_to_bigquery

# ---- Sample API response ----
SAMPLE_API_RESPONSE = {
    "Time Series (5min)": {
        "2025-11-15 12:00:00": {
            "1. open": "100",
            "2. high": "105",
            "3. low": "95",
            "4. close": "102",
            "5. volume": "1000"
        },
        "2025-11-15 12:05:00": {
            "1. open": "102",
            "2. high": "106",
            "3. low": "101",
            "4. close": "104",
            "5. volume": "1200"
        }
    }
}

SAMPLE_SERVICE_ACCOUNT = {
    "type": "service_account",
    "project_id": "test-project",
    "private_key_id": "fake_key_id",
    "private_key": "-----BEGIN PRIVATE KEY-----\nFAKE\n-----END PRIVATE KEY-----\n",
    "client_email": "test@test-project.iam.gserviceaccount.com",
    "client_id": "1234567890",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test@test-project.iam.gserviceaccount.com"
}

SAMPLE_BQ_CONFIG = {
    "dataset": "sales_dataset",
    "table": "sales",
    "location": "US"
}


# ---- Fixtures ----
@pytest.fixture
def tmp_json(tmp_path):
    """Creates a temporary JSON file with sample API response."""
    file_path = tmp_path / "extract_sample.json"
    with open(file_path, "w") as f:
        json.dump(SAMPLE_API_RESPONSE, f)
    return str(file_path)


# ---- Extract Tests ----
@patch("dags.etl.extract.requests.get")
def test_extract_from_api(mock_get, tmp_path):
    """Test API extraction saves JSON correctly."""
    mock_response = MagicMock()
    mock_response.json.return_value = SAMPLE_API_RESPONSE
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    output_path = extract_from_api("http://fake-url.com/api", base_dir=str(tmp_path))
    assert Path(output_path).exists()

    with open(output_path, "r") as f:
        data = json.load(f)
    assert data == SAMPLE_API_RESPONSE


# ---- Transform Tests ----
def test_transform_market_data(tmp_json):
    """Test transforming API JSON into normalized DataFrame JSON."""
    output_path = transform_market_data(tmp_json)
    assert Path(output_path).exists()

    df = pd.read_json(output_path)
    # Check transformed columns exist
    for col in ["timestamp", "open", "close", "high", "low", "volume", "price_change", "price_change_pct"]:
        assert col in df.columns
    assert len(df) == 2


# ---- Load Tests ----
@patch("dags.etl.load.bigquery.Client")
@patch("dags.etl.load.service_account.Credentials.from_service_account_info")
def test_load_to_bigquery(mock_creds, mock_bq_client, tmp_json):
    """Test loading transformed JSON into BigQuery (mocked)."""
    mock_creds.return_value = "fake_creds"

    # Mock BigQuery client
    mock_client_instance = MagicMock()
    mock_bq_client.return_value = mock_client_instance

    # Mock load job
    mock_job = MagicMock()
    mock_job.result.return_value = None
    mock_job.errors = None
    mock_job.output_rows = 2
    mock_client_instance.load_table_from_dataframe.return_value = mock_job

    loaded_rows = load_to_bigquery(
        file_path=tmp_json,
        gcp_service_account=SAMPLE_SERVICE_ACCOUNT,
        gcp_bigquery_config=SAMPLE_BQ_CONFIG
    )

    assert loaded_rows == 2
    mock_creds.assert_called_once_with(SAMPLE_SERVICE_ACCOUNT)
    mock_bq_client.assert_called_once()
    mock_client_instance.load_table_from_dataframe.assert_called_once()
