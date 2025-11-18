# Stock ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline for processing stock market data. The pipeline is designed to extract data from an external API, transform it into a structured format, and load it into a BigQuery database for further analysis. It is built with modularity and scalability in mind, making it easy to extend or adapt to other data sources.

## Features
- **Data Extraction**: Fetch stock market data from an external API (e.g., Alpha Vantage).
- **Data Transformation**: Clean, normalize, and enrich the data with derived metrics such as moving averages, price changes, and volatility.
- **Data Loading**: Store the processed data in Google BigQuery for further analysis and visualization.
- **Airflow Integration**: Orchestrate the ETL pipeline using Apache Airflow.
- **Unit Testing**: Comprehensive test coverage for all pipeline components to ensure reliability.
- **Continuous Integration**: Automated testing and validation using GitHub Actions.
- **Dockerized Deployment**: Run the entire pipeline in a containerized environment for consistency and portability.

## Project Structure
```
.
├── src/stock_etl_pipeline
│   ├── etl/
│   │   ├── extract.py       # Extract data from API
│   │   ├── transform.py     # Transform raw data
│   │   ├── load.py          # Load data into BigQuery
│   ├── main.py           # Airflow DAG definition
│   └── __init__.py           # Airflow DAG definition
├── dags/
│   ├── etl_dag.py      # Unit tests for extract module
├── tests/
│   ├── test_extract.py      # Unit tests for extract module
│   ├── test_transform.py    # Unit tests for transform module
│   ├── test_load.py         # Unit tests for load module
├── .github/workflows/
│   └── ci.yml               # GitHub Actions workflow for CI
├── requirements.txt         # Python dependencies
├── Dockerfile               # Docker setup for the project
├── docker-compose.yaml      # Docker Compose configuration
└── README.md                # Project documentation
```

## Getting Started

### Prerequisites
- Python 3.9+
- Docker (optional, for containerized deployment)
- Google Cloud credentials for BigQuery
- Google Cloud Database name strcuture
- Alpha Vantage API key

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/matiasgays/stock_etl_pipeline.git
   cd stock_etl_pipeline
   ```
2. Install dependencies:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```
3. Create .env file
   ```bash
   echo "AIRFLOW_UID=50000" > .env
   ```

### Running the Pipeline

This project requires specific Airflow Variables to be configured.  
You must load them in **JSON format** through the Airflow UI:
1. Start Airflow
   ```bash
   docker compose up -d
   ```
2. Navigate to **Admin → Variables**
3. Create a new variable
4. Paste the configuration as valid JSON:

Example:
{
  "api_key": "your_key",
  "endpoint": "https://api.example.com",
  "limit": 100
}


5. Run the Airflow DAG:
   - Start the Airflow scheduler and webserver.
   - Trigger the `etl_pipeline` DAG from the Airflow UI.

6. Open Big Query:
   - Check the data has been uploaded.

### Running Tests
Run the unit tests using pytest:
```bash
pytest
```

## CI/CD
This project uses GitHub Actions for continuous integration. The workflow is defined in `.github/workflows/ci.yml` and runs tests on every push or pull request to the `master` and `test-pr` branches.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Author
Matias Gays

