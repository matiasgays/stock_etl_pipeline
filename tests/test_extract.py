import json
import sys
from pathlib import Path

import requests
import pytest

# Add project root to sys.path so tests can import the package
root_dir = Path(__file__).resolve().parents[1]
sys.path.append(str(root_dir))

from dags.etl.extract import extract_from_api


def test_extract_from_api_success(monkeypatch):
    class FakeResponse:
        def raise_for_status(self):
            print("[DEBUG] FakeResponse.raise_for_status() called — no error")
            return None

        def json(self):
            data = {"data": [1, 2, 3]}
            print(f"[DEBUG] FakeResponse.json() returning: {data}")
            return data

    def fake_get(url, headers=None):
        print(f"[DEBUG] fake_get called with url={url}, headers={headers}")
        assert url == "http://example.com/data"
        return FakeResponse()

    monkeypatch.setattr("dags.etl.extract.requests.get", fake_get)

    print("[DEBUG] Calling extract_from_api()...")
    out = extract_from_api("http://example.com/data", headers={"a": "b"})
    print(f"[DEBUG] extract_from_api() returned raw output: {out}")

    parsed = json.loads(out)
    print(f"[DEBUG] Parsed JSON: {parsed}")

    assert parsed == {"data": [1, 2, 3]}
    print("[DEBUG] ✅ test_extract_from_api_success passed successfully")


def test_extract_from_api_raises_on_http_error(monkeypatch):
    class FakeResponse:
        def raise_for_status(self):
            print("[DEBUG] FakeResponse.raise_for_status() raising HTTPError")
            raise requests.HTTPError("bad")

        def json(self):
            print("[DEBUG] FakeResponse.json() called — returning empty dict")
            return {}

    def fake_get(url, headers=None):
        print(f"[DEBUG] fake_get called with url={url}, headers={headers}")
        return FakeResponse()

    monkeypatch.setattr("dags.etl.extract.requests.get", fake_get)

    print("[DEBUG] Expecting HTTPError from extract_from_api()...")
    with pytest.raises(requests.HTTPError):
        extract_from_api("http://example.com/data")
    print("[DEBUG] ✅ test_extract_from_api_raises_on_http_error passed successfully")
