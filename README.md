# Stock ETL Pipeline

A production-ready ETL pipeline built with **Apache Airflow**, designed to:

- Extract stock market data from **Alpha Vantage**
- Transform and standardize timestamps and metrics
- Load records into **Google BigQuery**
- Notify success via **Slack webhook**
- Use secure, validated configuration via `.env` or Airflow Variables

This project is fully Dockerized and optimized for production use.

---

## 🚀 Features

### 🟦 Data Extraction  
Fetch Time Series Stock Data from Alpha Vantage API.

### 🟩 Data Transformation  
Normalize and compute financial metrics:  
- Moving averages  
- Volatility  
- Minute-level consistency corrections  
- Clean column formatting  

### 🟥 Data Loading  
Write structured data into BigQuery using a validated GCP Service Account.

### ⚙️ Airflow DAG  
Daily orchestrated pipeline with task-by-task logic:

```
extract → transform → load → slack_notification
```

---

# 🚀 Quick Start

### 1. Clone the repo

```bash
git clone https://github.com/your-repo
cd stock-etl
```

### 2. Configure environment variables

Copy `.env.example` to `.env`:

```bash
cp .env.example .env
```

Your `.env` already includes all required variables:

```env
# Airflow
AIRFLOW_UID=1000

# GCP Service Account (path to JSON file)
GCP_CREDENTIALS=/opt/airflow/dags/resources/gcp_credentials.json

# Defaults (used only if Airflow Variables are not set)
DEFAULT_STOCK_SYMBOL=IBM
DEFAULT_ALPHA_VANTAGE_KEY=demo
DEFAULT_ALPHA_VANTAGE_INTERVAL=5min
DEFAULT_ALPHA_VANTAGE_FUNCTION=TIME_SERIES_INTRADAY
DEFAULT_ALPHA_VANTAGE_URL=https://www.alphavantage.co/query
DEFAULT_DATASET_NAME=time_series_stock_dataset
DEFAULT_TABLE_NAME=IBM
DEFAULT_DATASET_LOCATION=US
DEFAULT_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/XXX/YYY/ZZZ
```

If you place your GCP JSON in:

```
dags/resources/gcp_credentials.json
```

Airflow will automatically detect it and validate it.

---

### 3.1 Running Airflow

### Start the environment:

```bash
docker compose up -d
```

### Access UI:

```
http://localhost:8080
```

### Trigger the DAG:

`etl_pipeline`

---

### 3.2 Testing

```bash
pytest
```

Runs unit tests for extract, transform, and load stages.

---

## 📂 Project Structure

```text
.
├── src/stock_etl_pipeline
│   ├── etl/
│   │   ├── extract.py       # Pull stock data from API
│   │   ├── transform.py     # Transform market data
│   │   ├── load.py          # Load into BigQuery
│   ├── utils/
│   │   ├── config.py        # Unified configuration loader + validation
│   │   ├── slack.py         # Slack webhook notification helper
│   └── __init__.py
├── dags/
│   ├── etl_dag.py           # Airflow DAG definition
├── dags/resources/
│   └── gcp_credentials.json # Optional location for Service Account JSON
├── tests/
│   ├── test_etl.py
├── .github/workflows/
│   └── ci.yml               # GitHub Actions CI
├── requirements.txt
├── docker-compose.yaml
├── Dockerfile
└── README.md
```

---

# 🏗️ Architecture

```
extract → transform → load → slack_notification
```

- **extract.py:** Calls Alpha Vantage  
- **transform.py:** Normalizes and reshapes stock data  
- **load.py:** Validates and writes into BigQuery  
- **slack.py:** Sends success message to Slack  
- **config.py:** Unified config loader with validation  

---

# ⚙️ Configuration System (IMPORTANT)

The pipeline pulls configuration in this priority order:

```
1. Airflow Variables
2. .env variables  
3. Internal defaults
```

This allows flexible deployment across **local**, **CI/CD**, or **Cloud Composer**.

---

## 🔐 1. Required Airflow Variables / Environment Variables

### ### Alpha Vantage
| Airflow Variable | .env Variable | Default | Description |
|--------|---------------|-------------|-------------
| alpha_vantage_api_key | `ALPHA_VANTAGE_KEY` | `demo` | API key for Alpha Vantage |
| stock_symbol | `ALPHA_VANTAGE_STOCK_SYMBOL` | `IBM` | Stock ticker |
| alpha_vantage_interval | `ALPHA_VANTAGE_INTERVAL` | `5min` | Time interval between data points |
| alpha_vantage_function | `ALPHA_VANTAGE_FUNCTION` | `TIME_SERIES_INTRADAY` | Time series |
| alpha_vantage_url | `ALPHA_VANTAGE_URL` | `https://www.alphavantage.co/query` | Base URL |

### ### BigQuery
| Airflow Variable | .env Variable | Default | Description |
|------------------|--------------|-------------|-------------
| `bigquery_dataset` | `DATASET_NAME` | `default_dataset` | BigQuery dataset |
| `bigquery_table` | `TABLE_NAME` | `default_table` | Table name |
| `bigquery_location` | `DATASET_LOCATION` | `US` | Location (“US”, “EU”, etc.) |

### ### Airflow
| Airflow Variable | .env Variable | Default | Description |
|------------------|--------------|-------------|-------------|
| `------` | `AIRFLOW_UID` | `0` | Airflow UID variable dataset |

---

# 🟨 GCP SERVICE ACCOUNT — 2 VALID OPTIONS

The `config.py` supports 2 ways to load the Service Account JSON.

The pipeline validates:

- presence of required keys  
- correct PEM format  
- JSON structure  

---

## **OPTION 1 — Store JSON directly in Airflow**

Go to:

```
Airflow → Admin → Variables
```

Create:

**Key:** `gcp_credentials_json`  
**Value:** JSON contents (copy/paste full service account)

Example: 
```json
{
  "type": "service_account",
  "project_id": "your-project",
  "private_key_id": "...",
  "private_key": "-----BEGIN PRIVATE KEY-----\nabc...\n-----END PRIVATE KEY-----\n",
  "client_email": "your-sa@project.iam.gserviceaccount.com",
  "client_id": "...",
  "auth_uri": "...",
  "token_uri": "...",
  "auth_provider_x509_cert_url": "...",
  "client_x509_cert_url": "..."
}
```

No need to mount files into containers.

---

## **OPTION 2 — Store JSON file locally and reference via .env**

Add to `.env`:

```
GCP_CREDENTIALS=/opt/airflow/dags/resources/gcp_credentials.json
```

Place the JSON contents (copy/paste full service account) here locally:

```
dags/resources/gcp_credentials.json
```

This is perfect for local development.

---

# 🔔 Slack Notifications

The DAG includes a task:

```
success_notification
```

Handled by:

- `utils/slack.py`

To enable Slack:

Add to `.env`:

```
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/XXX/YYY/ZZZ
```

Or create Airflow Variable:

```
Key: slack_webhook_url
Value: https://hooks.slack.com/services/...
```

The DAG will send a message on success.

---

# 📦 CI/CD (GitHub Actions)

- Runs tests on push / PR  
- Ensures Airflow code imports properly  
- Validates Python formatting

---

# 🛠️ Troubleshooting

### ❌ GCP credentials missing  
Ensure:

```
/opt/airflow/dags/resources/gcp_credentials.json
```

is mounted via Docker AND present inside the container:

```bash
docker exec -it airflow-webserver ls /opt/airflow/dags/resources
```

### ❌ Slack not sending  
Check:

- Airflow Variable: `slack_webhook_url`
- OR `.env`: `SLACK_WEBHOOK_URL`

---

# 👤 Author

**Matías Gays**

MIT License.

