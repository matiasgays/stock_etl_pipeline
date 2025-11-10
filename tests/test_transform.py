import json
import sys
from pathlib import Path

import pandas as pd
import pytest

# Add project root to sys.path so tests can import the package
root_dir = Path(__file__).resolve().parents[1]
sys.path.append(str(root_dir))

from dags.etl.transform import transform_market_data


def build_time_series(entries):
    # entries: list of (timestamp_str, values_dict)
    ts = {ts: vals for ts, vals in entries}
    return {"Time Series (5min)": ts}


def test_transform_basic_sanity():
    entries = [
        ("2025-01-01 09:10:00", {"1. open": "12", "2. high": "13", "3. low": "11", "4. close": "12.5", "5. volume": "150"}),
        ("2025-01-01 09:00:00", {"1. open": "10", "2. high": "12", "3. low": "9", "4. close": "11", "5. volume": "100"}),
        ("2025-01-01 09:05:00", {"1. open": "11", "2. high": "12.5", "3. low": "10", "4. close": "11.5", "5. volume": "120"}),
    ]

    raw = build_time_series(entries)
    raw_str = json.dumps(raw)

    out_json = transform_market_data(raw_str)
    df = pd.read_json(out_json, orient="records")

    print("\n=== Transformed DataFrame ===")
    print(df[["timestamp", "open", "close", "price_change", "ma_3"]])
    print("\nExpected order (ascending):", sorted(pd.to_datetime(df["timestamp"]).tolist()))
    print("Actual order:", list(pd.to_datetime(df["timestamp"]).tolist()))

    assert len(df) == 3
    assert list(pd.to_datetime(df["timestamp"])) == sorted(pd.to_datetime(df["timestamp"]))

    first = df.iloc[0]
    expected_price_change = first["close"] - first["open"]
    print(f"\nExpected price_change for first row: {expected_price_change}, Actual: {first['price_change']}")
    assert pytest.approx(first["price_change"]) == expected_price_change


def test_transform_handles_invalid_entries():
    # include an entry with invalid timestamp and non-numeric values
    entries = [
        ("invalid-time", {"1. open": "not-a-number", "2. high": "x", "3. low": "y", "4. close": "z", "5. volume": "n/a"}),
        ("2025-01-01 09:00:00", {"1. open": "10", "2. high": "12", "3. low": "9", "4. close": "11", "5. volume": "100"}),
    ]

    raw = build_time_series(entries)
    raw_str = json.dumps(raw)

    out_json = transform_market_data(raw_str)
    df = pd.read_json(out_json, orient="records")

    # invalid timestamp row should be dropped, leaving only the valid row
    assert len(df) == 1

    # numeric coercion should have produced numeric types (or NaN) for numeric columns
    assert df["open"].iloc[0] == 10
    assert df["volume"].iloc[0] == 100
