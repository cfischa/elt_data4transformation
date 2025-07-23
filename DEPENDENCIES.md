# BnB Data4Transformation - Dependency Documentation

## Environment Setup Complete ✅

**Environment Name:** `bnb_data4transformation`  
**Python Version:** 3.12.11  
**Location:** `C:\Users\chris\anaconda3\envs\bnb_data4transformation\python.exe`

## Core Dependencies Analysis

Based on comprehensive codebase analysis, the following dependencies have been identified and installed:

### 🔧 **Core Framework & Orchestration**
- **Apache Airflow 2.9.1** - Workflow orchestration platform
  - `apache-airflow-providers-postgres` - PostgreSQL integration
  - `apache-airflow-providers-http` - HTTP operators
  - `apache-airflow-providers-celery` - Celery executor
  - `apache-airflow-providers-common-sql` - SQL utilities
  - `apache-airflow-providers-slack` - Slack notifications

### 🗄️ **Database & Storage**
- **ClickHouse Connect 0.8.18** - Main analytics database driver
- **psycopg2-binary 2.9.10** - PostgreSQL adapter (Airflow metadata)
- **SQLAlchemy 1.4.54** - SQL toolkit and ORM
- **MinIO 7.2.16** - S3-compatible object storage
- **boto3 1.39.11** - AWS SDK for additional S3 operations

### 📊 **Data Processing & Analytics**
- **pandas 2.3.1** - Data manipulation and analysis
- **numpy 2.3.1** - Numerical computing
- **pyarrow 17.0.0** - Columnar data format
- **polars 1.31.0** - Fast DataFrame library
- **scikit-learn 1.7.1** - Machine learning
- **scipy 1.16.0** - Scientific computing

### 🌐 **HTTP & Async Operations**
- **aiohttp 3.9.5** - Async HTTP client/server
- **httpx 0.27.0** - Modern HTTP client
- **requests 2.32.2** - Traditional HTTP library
- **aiofiles 24.1.0** - Async file operations

### ⚡ **Data Validation & Configuration**
- **pydantic 2.5.3** - Data validation using Python type hints
- **python-dotenv 1.0.0** - Environment variable management
- **tenacity 8.2.0** - Retry library

### 🕷️ **Web Scraping**
- **beautifulsoup4 4.12.3** - HTML/XML parsing
- **scrapy 2.11.1** - Web scraping framework
- **selenium 4.17.0** - Browser automation

### 🖥️ **CLI & User Interface**
- **typer 0.9.0** - Modern CLI framework
- **rich 13.9.4** - Rich text and beautiful formatting

### 🧪 **Testing & Quality Assurance**
- **pytest 7.4.4** - Testing framework
- **pytest-asyncio 0.23.8** - Async testing support
- **pytest-cov 6.0.0** - Coverage testing
- **black 24.4.2** - Code formatter
- **ruff 0.12.4** - Fast Python linter
- **isort 5.13.2** - Import sorting
- **mypy 1.17.0** - Static type checker
- **pre-commit 3.8.0** - Git hooks framework

### 📊 **Visualization & Analysis**
- **matplotlib 3.10.1** - Plotting library
- **seaborn 0.13.2** - Statistical data visualization
- **jupyterlab 4.4.5** - Interactive development environment
- **ipykernel 6.28.0** - Jupyter kernel

### 📈 **Monitoring & Logging**
- **structlog 25.4.0** - Structured logging
- **python-json-logger 3.3.0** - JSON logging formatter
- **prometheus-client 0.19.0** - Metrics collection

### 🏗️ **Task Processing**
- **celery 5.5.3** - Distributed task queue
- **redis 5.2.1** - In-memory data store
- **flower 2.0.1** - Celery monitoring

### 🌍 **External APIs & Data Sources**
- **pystatis 0.5.4** - German Federal Statistical Office API

### 🔧 **Utility Libraries**
- **orjson 3.11.0** - Fast JSON library
- **pendulum 3.1.0** - Date/time manipulation
- **click 8.1.7** - Command line interface creation

## Project Structure Dependencies

### 📁 **Directory Analysis**
```
/connectors/          → API connectors (aiohttp, httpx, pydantic)
/elt/                 → Extract-Load-Transform (clickhouse-connect, pandas)
/dags/                → Airflow DAGs (apache-airflow, all providers)
/scraping/            → Web scraping (scrapy, beautifulsoup4, selenium)
/tests/               → Testing (pytest, pytest-asyncio, pytest-cov)
/pipeline/            → CLI tools (typer, rich)
/dbt_project/         → dbt transformations (requires dbt-clickhouse)
```

### 🔍 **Code Analysis Findings**

1. **Main Application** (`main.py`)
   - Uses `DawumConnector` from data_acquisition
   - Requires logging configuration
   - File persistence utilities

2. **ELT Pipeline** (`elt/`)
   - ClickHouse bulk loading capabilities
   - Metadata tracking with Pydantic models
   - Structured logging with contextual information

3. **Airflow DAGs** (`dags/`)
   - TaskFlow API decorators
   - Multiple provider integrations
   - Database sensors and operators

4. **Connectors** (`connectors/`)
   - Async HTTP clients with rate limiting
   - Pydantic configuration models
   - Retry logic with tenacity

5. **Web Scraping** (`scraping/`)
   - Scrapy spiders with custom settings
   - BeautifulSoup parsing utilities
   - Selenium for JavaScript-heavy sites

## VS Code Integration

The environment is automatically configured in VS Code:
```json
{
    "python.defaultInterpreterPath": "C:\\Users\\chris\\anaconda3\\envs\\bnb_data4transformation\\python.exe",
    "python.terminal.activateEnvironment": true
}
```

## Verification Commands

Test the installation:
```bash
# Activate environment
conda activate bnb_data4transformation

# Test core imports
python -c "import pandas, clickhouse_connect, pydantic; print('✅ Core OK')"

# Test Airflow (requires configuration)
python -c "from airflow.decorators import dag, task; print('✅ Airflow imports OK')"

# Test web scraping
python -c "import scrapy, bs4, selenium; print('✅ Scraping OK')"

# Run tests
pytest tests/ -v

# Check code quality
black --check .
ruff check .
mypy connectors elt
```

## Next Steps

1. **Configure Airflow**: Set up `airflow.cfg` and database connections
2. **Environment Variables**: Ensure all API keys are set in `.env`
3. **Docker Services**: Start ClickHouse, MinIO, and other infrastructure
4. **dbt Setup**: Install `dbt-clickhouse` adapter if needed
5. **Run Pipeline**: Execute `make pipeline` to test end-to-end flow

## Environment Recovery

This environment can be recreated using:
```bash
conda env create -f environment.yml
```

All dependencies are now properly installed and documented! 🎉
