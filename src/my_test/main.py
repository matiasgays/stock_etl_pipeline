#from dags.etl.extract import extract_from_api
from dags.etl.transform import transform_market_data
#from my_test.load import load

def main():
    api_url = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=CQGF5X46T7ZFV6I4"
    headers = {"Accept": "application/json"}

    #print(raw_data)
    clean_df = transform_market_data()
    #load()
    print(clean_df.head())

if __name__ == "__main__":
    main()
