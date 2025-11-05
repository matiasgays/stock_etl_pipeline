import os
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

def load_to_bigquery(df, project_id):

    # Path to your credentials file
    #key_path = "/home/mello/stock/src/stock/.env/service_account.json"
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))  # go up from dags/etl/ to project root
    service_account_path = os.path.join(base_dir, "config", "service_account.json")

    print("Service account path:", service_account_path)

    # Create credentials object
    credentials = service_account.Credentials.from_service_account_file(service_account_path)
    print(credentials)
    
    client = bigquery.Client(credentials=credentials, project=project_id)

    # Dataset ID
    dataset_id = f"{project_id}.sales_dataset"
    # Now load the data
    table_id = f"{dataset_id}.sales"

    # Create the dataset if it doesn’t exist
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"  # or your region, e.g. "southamerica-east1"

    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset {dataset.dataset_id} created or already exists.")

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"Loaded {job.output_rows} rows into {table_id}.")

    # Run a query
    query = "SELECT * FROM `productos-320620.sales_dataset.sales`"
    df = client.query(query).to_dataframe()

    print(df)

def load():
    print("Loading data...")
    return