# 📈 Stock ETL Pipeline

A **production-ready ETL pipeline** built with **Apache Airflow**, fully Dockerized and designed to:

- 🔵 **Extract** stock market data from **Alpha Vantage**  
- 🟢 **Transform** and standardize timestamps + compute financial metrics  
- 🔴 **Load** the results into **Google BigQuery**  
- 📣 **Notify** via Slack webhook  
- 🔐 **Securely manage configuration** via `.env` or Airflow Variables  

---

## 🚀 Features

### 🟦 Data Extraction  
Retrieve Time Series Stock Data using the Alpha Vantage API.

### 🟩 Data Transformation  
Clean and prepare the raw data, including:
- Moving averages  
- Volatility  
- Minute-level corrections  
- Column normalization  

### 🟥 Data Loading  
Insert validated, well-structured data into **BigQuery** using a secure GCP Service Account.

### ⚙️ Airflow DAG  
Daily scheduled pipeline:

```
extract → transform → load → slack_notification
```

---

# 📦 Requirements

To run this project in **local**, **Docker**, or **CI/CD**, ensure you meet the following:

## 🔧 System Requirements
- **Docker** ≥ 20+
- **Docker Compose** ≥ v2+
- **Python** ≥ 3.10 (only required for running tests locally)
- **Git** ≥ 2.0

---

## 🐍 Python Dependencies

Defined in `requirements.txt`:

```
apache-airflow==2.10.1
pandas>=2.0.0
requests>=2.31.0
google-cloud-bigquery>=3.15.0
google-auth>=2.25.0
slack_sdk>=3.27.0
pytest>=8.0.0
python-dotenv>=1.0.1
```

---

## ⭐ API & Platform Requirements

### 📊 Alpha Vantage
- A valid **Alpha Vantage API key**
- Free tier works for daily/minute data (limited)
- Paid tier recommended for reliable intraday queries

### 🟨 Google Cloud (BigQuery)
You need:

- A **GCP Project**
- A **BigQuery Dataset**
- A **Service Account** with either:
  - **Recommended:** `BigQuery Admin`
  - **Minimum roles:**
    - `BigQuery Data Editor`
    - `BigQuery Job User`

### 💬 Slack
- A Slack workspace
- An **Incoming Webhook URL**

---

# 🚀 Quick Start

## 1️⃣ Clone the repo

```bash
git clone https://github.com/your-repo
cd stock-etl

```
## 2️⃣ Install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## 3️⃣ Configure environment variables

Copy `.env.example` to `.env`:

```bash
cp .env.example .env
```

Your `.env` already includes all default variables:

```env
# Airflow
AIRFLOW_UID=1000

# Defaults (used only if Airflow Variables are not set)
STOCK_SYMBOL=IBM
ALPHA_VANTAGE_KEY=demo
ALPHA_VANTAGE_INTERVAL=5min
ALPHA_VANTAGE_FUNCTION=TIME_SERIES_INTRADAY
ALPHA_VANTAGE_URL=https://www.alphavantage.co/query
DATASET_NAME=time_series_stock_dataset
TABLE_NAME=IBM
DATASET_LOCATION=US
```

---

## 4️⃣ Set Up Google Cloud Service Account (GCP)

Your BigQuery loader requires a **Service Account** with a **JSON key**.

### **Step 1 — Open GCP Console**  
https://console.cloud.google.com/

### **Step 2 — Navigate to:**  
**IAM & Admin → Service Accounts**

### **Step 3 — Create New Service Account**
- **Name:** `your-name`
- **Recommended Role:** `BigQuery Admin`  
- **Minimal Roles (if you prefer least privilege):**
  - `BigQuery Data Editor`
  - `BigQuery Job User`

### **Step 4 — Create Key → JSON**
Download the JSON file.

---

### **Step 5 — Choose a configuration method**

The `config.py` supports 2 ways to load the Service Account JSON.

The pipeline validates:

- presence of required keys  
- correct PEM format  
- JSON structure  

---

#### **Option A — Store JSON directly in Airflow**

Go to:

```
Airflow → Admin → Variables
```

Create:
| Key                   | Value                     |
|----------------------|---------------------------|
| `gcp_credentials_json` | JSON contents (copy/paste full service account) |

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

#### **Option B — Store JSON file locally and reference via .env**

Add to `.env`:

```
GCP_CREDENTIALS=/opt/airflow/*(your path)*
```
Example:
```
/opt/airflow/config/gcp_credentials.json
```

Place the JSON contents (copy/paste full service account) in your path.
Example:

```
config/gcp_credentials.json
```

This is perfect for local development.

---

## 5️⃣ Setup Slack Webhook

This pipeline sends a Slack message when the ETL completes successfully.

### **Step 1 — Visit Slack API**  
https://api.slack.com/apps

### **Step 2 — Create New App → “From Scratch”**

### **Step 3 — Enable Incoming Webhooks**  
Left sidebar → **Incoming Webhooks → ON**

### **Step 4 — Add Webhook to a Channel**  
Slack generates a URL like:
https://hooks.slack.com/services/XXX/YYY/ZZZ

### **Step 5 — Choose a configuration method**

#### **Option A — Paste URL into Airflow Variables**  
Airflow UI → **Admin → Variables**

| Key                   | Value                     |
|----------------------|---------------------------|
| `slack_webhook_url` | *(paste the url here)* |

The DAG will send a message on success.

---

#### **Option B — Store the de URL locally**

Add to `.env`:

```
SLACK_WEBHOOK_URL= *(paste the url here)*
```

---

## 6️⃣ Running Airflow

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

## 🧪 Testing

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
├── config/
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

# ⚙️ Configuration System

The pipeline pulls configuration in this priority order:

```
1. Airflow Variables
2. .env variables  
3. Internal defaults
```

This allows flexible deployment across **local**, **CI/CD**, or **Cloud Composer**.

---

## Variables

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

### ### GCP Service account
| Airflow Variable | .env Variable | Default | Description |
|------------------|--------------|-------------|-------------|
| `gcp_credentials_json` | `GCP_CREDENTIALS` | `------` | GCP service account JSON |

### ### Slack
| Airflow Variable | .env Variable | Default | Description |
|------------------|--------------|-------------|-------------|
| `slack_webhook_url` | `SLACK_WEBHOOK_URL` | `------` | Slack URL |

---

# 📦 CI/CD (GitHub Actions)

- Runs tests on push / PR  
- Ensures Airflow code imports properly  
- Validates Python formatting

---

# 🛠️ Troubleshooting

### ❌ GCP credentials missing  
Check:

- Airflow Variable: `gcp_credentials`
- OR ensure:

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

