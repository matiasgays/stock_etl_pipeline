import pandas as pd
import json
import logging

# ---- Setup logger ----
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def transform_market_data(input_path: str) -> str:
    """
    Transform raw market data JSON into a normalized, enriched DataFrame and save as JSON.

    Args:
        input_path: Path to raw JSON file.

    Returns:
        Path to transformed JSON file.
    """
    # ---- 1. Load JSON ----
    with open(input_path, "r") as f:
        raw_json = json.load(f)

    time_series = raw_json.get("Time Series (5min)")
    if not time_series:
        raise ValueError("Input JSON does not contain 'Time Series (5min)'.")

    df = pd.DataFrame.from_dict(time_series, orient="index")
    df.index.name = "timestamp"
    df.reset_index(inplace=True)

    # ---- 2. Normalize Column Names ----
    df.columns = df.columns.str.lower().str.strip()
    rename_map = {
        "1a. open (usd)": "open",
        "2a. high (usd)": "high",
        "3a. low (usd)": "low",
        "4a. close (usd)": "close",
        "5. volume": "volume",
        "6. market cap (usd)": "market_cap",
        "1. open": "open",
        "2. high": "high",
        "3. low": "low",
        "4. close": "close",
        "5. volume": "volume",
    }
    df = df.rename(columns={c: rename_map.get(c, c) for c in df.columns})
    
    # ---- 3. Timestamp Handling ----
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    # ---- 4. Convert Numeric Columns ----
    numeric_cols = ["open", "high", "low", "close", "volume"]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

    # ---- 5. Derived Metrics ----
    df["price_change"] = df["close"] - df["open"]
    df["price_change_pct"] = (df["close"] - df["open"]) / df["open"] * 100
    df["high_low_range"] = df["high"] - df["low"]
    df["high_low_pct"] = (df["high"] - df["low"]) / df["low"] * 100
    df["ma_3"] = df["close"].rolling(window=3).mean()
    df["ma_7"] = df["close"].rolling(window=7).mean()
    df["volatility_3"] = df["close"].pct_change().rolling(window=3).std() * 100

    # ---- 6. Date Parts ----
    df["year"] = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month
    df["day"] = df["timestamp"].dt.day
    df["day_of_week"] = df["timestamp"].dt.day_name()

    # ---- 7. Save Transformed JSON ----
    output_path = input_path.replace("extract_", "transform_")
    
    json_str = df.to_json(orient="records")

    with open(output_path, "w") as f: 
        f.write(json_str)

    logger.info("Transform saved → %s", output_path)
    return output_path