import json
import tempfile
import os
import pandas as pd
import types
import pytest
import sys
from pathlib import Path

# Add project root to sys.path
root_dir = Path(__file__).resolve().parents[1]
sys.path.append(str(root_dir))

from dags.etl.load import load_to_bigquery

class FakeJob:
    def __init__(self, output_rows=0, errors=None):
        self.output_rows = output_rows
        self.errors = errors

    def result(self, timeout=None):
        return self


class FakeClient:
    def __init__(self):
        self.created_datasets = []
        self.loaded_df = None

    def create_dataset(self, dataset, exists_ok=True):
        # record that dataset was created
        # dataset may be a simple object; store its id/str
        try:
            self.created_datasets.append(dataset.dataset_id)
        except Exception:
            self.created_datasets.append(str(dataset))

    def load_table_from_dataframe(self, df, table_id, job_config=None):
        # capture dataframe for assertions
        self.loaded_df = df.copy()
        return FakeJob(output_rows=len(df), errors=None)

    def query(self, sql):
        class Q:
            def to_dataframe(self):
                return pd.DataFrame([{"sample": 1}])

        return Q()


def make_fake_bigquery_module(fake_client):
    fake_bq = types.SimpleNamespace()

    def client_ctor(credentials, project):
        return fake_client

    fake_bq.Client = client_ctor

    class Dataset:
        def __init__(self, dataset_id):
            self.dataset_id = dataset_id
            self.location = None

    fake_bq.Dataset = Dataset

    class LoadJobConfig:
        def __init__(self):
            self.write_disposition = None

    fake_bq.LoadJobConfig = LoadJobConfig

    class WriteDisposition:
        WRITE_APPEND = "WRITE_APPEND"

    fake_bq.WriteDisposition = WriteDisposition

    return fake_bq


def test_load_success(monkeypatch, tmp_path):
    # create a temporary credentials file and set env var
    cred_file = tmp_path / "sa.json"
    cred_file.write_text('{"type": "service_account", "project_id": "proj"}')
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(cred_file))

    # sample JSON with out-of-order timestamps
    records = [
        {"timestamp": "2025-01-02T10:00:00Z", "value": 2},
        {"timestamp": "2025-01-01T09:00:00Z", "value": 1},
    ]
    json_str = json.dumps(records)

    # prepare fake client and inject fake bigquery module into the target module
    fake_client = FakeClient()
    fake_bq = make_fake_bigquery_module(fake_client)
    monkeypatch.setattr("dags.etl.load.bigquery", fake_bq)

    # patch service account credential loader to a simple stub
    monkeypatch.setattr(
        "dags.etl.load.service_account.Credentials.from_service_account_file",
        lambda path: object(),
    )

    rows = load_to_bigquery(json_str, project_id="proj")

    assert rows == 2

    # ensure the dataframe passed to load_table_from_dataframe is sorted ascending by timestamp
    assert list(fake_client.loaded_df["value"]) == [1, 2]


def test_missing_credentials_raises(monkeypatch):
    # ensure env var is not present
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)

    records = [{"timestamp": "2025-01-01T09:00:00Z", "value": 1}]
    json_str = json.dumps(records)

    with pytest.raises(EnvironmentError):
        load_to_bigquery(json_str, project_id="proj")

