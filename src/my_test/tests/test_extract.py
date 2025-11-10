import json
import sys
from pathlib import Path

import requests
import pytest

# Add project root to sys.path so tests can import the package
root_dir = Path(__file__).resolve().parents[1]
sys.path.append(str(root_dir))

from dags.etl.extract import extract_from_api


class _FakeResponse:
    def __init__(self, data=None, raise_error=False):
        self._data = data or {}
        self._raise = raise_error

    def raise_for_status(self):
        if self._raise:
            raise requests.HTTPError("bad")

    def json(self):
        return self._data


def _make_get(json_data=None, raise_error=False, expected_url=None, expected_headers=None):
    def _get(url, headers=None):
        if expected_url is not None:
            assert url == expected_url
        if expected_headers is not None:
            assert headers == expected_headers
        return _FakeResponse(json_data, raise_error)
    return _get


def test_extract_from_api_success(monkeypatch):
    monkeypatch.setattr(
        "dags.etl.extract.requests.get",
        _make_get(json_data={"data": [1, 2, 3]}, expected_url="http://example.com/data", expected_headers={"a": "b"}),
    )

    out = extract_from_api("http://example.com/data", headers={"a": "b"})
    assert json.loads(out) == {"data": [1, 2, 3]}


def test_extract_from_api_raises_on_http_error(monkeypatch):
    monkeypatch.setattr("dags.etl.extract.requests.get", _make_get(raise_error=True))

    with pytest.raises(requests.HTTPError):
        extract_from_api("http://example.com/data")
