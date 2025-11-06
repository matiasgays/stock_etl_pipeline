import os
import pandas as pd
import json

def transform_market_data(raw_str) -> pd.DataFrame:
    raw_json = json.loads(raw_str)
    time_series = raw_json.get("Time Series (5min)", {})
    df = pd.DataFrame.from_dict(time_series, orient="index")
    df.index.name = "timestamp"
    df.reset_index(inplace=True)

    # Rename columns to simpler names
    df = df.rename(columns={
        "1a. open (USD)": "open",
        "2a. high (USD)": "high",
        "3a. low (USD)": "low",
        "4a. close (USD)": "close",
        "5. volume": "volume",
        "6. market cap (USD)": "market_cap"
    })

    # Handle case where names are just "1. open", "2. high", etc.
    for old, new in {
        "1. open": "open",
        "2. high": "high",
        "3. low": "low",
        "4. close": "close",
        "5. volume": "volume"
    }.items():
        if old in df.columns:
            df = df.rename(columns={old: new})
    
    # Normalize column names
    df.columns = df.columns.str.lower()
    if "time" in df.columns:
        df = df.rename(columns={"time": "timestamp"})
    
    # Clean timestamp
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")

    # Convert numeric columns safely
    numeric_cols = ["open", "high", "low", "close", "volume"]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

    # Derived metrics
    df["price_change"] = df["close"] - df["open"]
    df["price_change_pct"] = (df["close"] - df["open"]) / df["open"] * 100
    df["high_low_range"] = df["high"] - df["low"]
    df["high_low_pct"] = (df["high"] - df["low"]) / df["low"] * 100

    # Trend indicators
    df["ma_3"] = df["close"].rolling(window=3).mean()
    df["ma_7"] = df["close"].rolling(window=7).mean()
    df["volatility_3"] = df["close"].pct_change().rolling(window=3).std() * 100

    # Date parts
    df["year"] = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month
    df["day"] = df["timestamp"].dt.day
    df["day_of_week"] = df["timestamp"].dt.day_name()
    print(df.head())

    return df.to_json(orient="records")

def transform():
    # Extract and normalize the time series data
    output_dir = os.path.dirname(__file__)
    #with open(os.path.join(output_dir, "response.json"), "r") as f:
    #    raw_json = json.load(f)
    print("Transforming data...")
    return