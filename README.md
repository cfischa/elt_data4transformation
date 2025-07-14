# BnB Data4Transformation - Political Data ELT Pipeline

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Extraction    │    │   Raw Storage   │
│                 │    │                 │    │                 │
│ • DAWUM API     │───▶│ • Connectors    │───▶│ • MinIO/S3      │
│ • Destatis API  │    │ • Scrapy        │    │ • JSON/Parquet  │
│ • Eurostat API  │    │ • BeautifulSoup │    │ • ClickHouse    │
│ • GESIS API     │    │ • Async/Batch   │    │   raw schema    │
│ • SOEP API      │    │                 │    │                 │
│ • Web Scraping  │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Orchestration  │    │ Transformation  │    │  Analytics DWH  │
│                 │    │                 │    │                 │
│ • Airflow 2.9+  │───▶│ • dbt-clickhouse│───▶│ • ClickHouse    │
│ • DAG Scheduler │    │ • SQL Models    │    │ • Star Schema   │
│ • Task Sensors  │    │ • Tests & Docs  │    │ • Marts/Cubes   │
│ • Monitoring    │    │ • Incremental   │    │ • Data Quality  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🎯 Overview

Production-grade ELT pipeline for political polling and socioeconomic data aggregation. Built with modern Python, Airflow orchestration, ClickHouse analytics warehouse, and dbt transformations.

### Key Features
- **Async API Connectors**: DAWUM, Destatis, Eurostat, GESIS, SOEP
- **Web Scraping**: Scrapy framework for sites without APIs
- **Object Storage**: MinIO S3-compatible for raw data lake
- **Analytics Warehouse**: ClickHouse for high-performance analytics
- **Transform Layer**: dbt-core with ClickHouse adapter
- **Orchestration**: Apache Airflow with TaskFlow API
- **Monitoring**: Prometheus metrics + data quality tests
- **Development**: Poetry, pre-commit, pytest, type hints

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- Poetry (or use provided Makefile)

### 1. Initialize Project
```bash
# Clone and setup
git clone <repository>
cd bnb_data4transformation

# Install dependencies and hooks
make init

# Copy environment template
cp .env.example .env
# Edit .env with your API keys and credentials
```

### 2. Start Infrastructure
```bash
# Start all services (ClickHouse, Airflow, MinIO, etc.)
make up && make smoke   # start stack and verify health

# Check service health
make status

# Initialize ClickHouse schemas
make init-db

# Access services:
# - Airflow UI: http://localhost:8081 (airflow/airflow)
# - ClickHouse: http://localhost:8124
# - MinIO Console: http://localhost:9003 (minioadmin/minioadmin)
# - MinIO API: http://localhost:9002
# - Jupyter: http://localhost:8888?token=admin
```

### 3. Run Sample Pipeline
```bash
# Load sample DAWUM data
make load-local

# Run dbt models
make dbt-run

# Access services:
# - Airflow UI: http://localhost:8081 (airflow/airflow)
# - ClickHouse: http://localhost:8124
# - MinIO Console: http://localhost:9003 (minioadmin/minioadmin)
# - MinIO API: http://localhost:9002
# - Jupyter: http://localhost:8888?token=admin
```

## 📁 Project Structure

```
├── README.md                 # This file
├── docker-compose.yml        # Infrastructure orchestration
├── .env.example              # Environment variables template
├── pyproject.toml            # Python dependencies & tools
├── Makefile                  # Common development tasks
├── 
├── dags/                     # Airflow DAG definitions
│   ├── extract_api_dag.py    # API data extraction
│   ├── extract_scrape_dag.py # Web scraping jobs
│   ├── load_clickhouse_dag.py # Data loading
│   ├── dbt_transform_dag.py  # dbt transformations
│   └── dbt_transformation_dag.py # dbt orchestration
├── 
├── connectors/               # Data source connectors
│   ├── __init__.py
│   ├── base_connector.py     # Abstract base class
│   ├── dawum_connector.py    # DAWUM API client
│   ├── destatis_connector.py # Destatis API client  
│   ├── eurostat_connector.py # Eurostat API client
│   ├── gesis_connector.py    # GESIS API client
│   └── soep_connector.py     # SOEP API client
├── 
├── elt/                      # Extract-Load-Transform utilities
│   ├── __init__.py
│   ├── loader_clickhouse.py  # ClickHouse bulk loader
│   ├── metadata.py          # Data schemas & models
│   └── utils/
│       ├── __init__.py
│       ├── logging_config.py # Structured logging
│       └── persistence.py   # File I/O helpers
├── 
├── scraping/                 # Web scraping framework
│   ├── __init__.py
│   ├── settings.py          # Scrapy configuration
│   └── spiders/
│       ├── __init__.py
│       └── example_spider.py # Template spider
├── 
├── dbt_project/             # dbt transformation project
│   ├── dbt_project.yml      # dbt configuration
│   ├── models/
│   │   ├── raw/             # Raw data models
│   │   ├── staging/         # Staging transformations
│   │   └── marts/           # Business logic models
│   ├── macros/              # Reusable SQL macros
│   ├── seeds/               # Static reference data
│   └── tests/               # Data quality tests
├── 
├── tests/                   # Test suite
│   ├── __init__.py
│   ├── unit/                # Unit tests
│   │   ├── __init__.py
│   │   └── test_connectors.py
│   └── integration/         # Integration tests
│       ├── __init__.py
│       └── test_pipeline.py
├── 
├── notebooks/               # Jupyter analysis notebooks
├── data/                    # Local data storage
│   ├── raw/                 # Raw extracted data
│   ├── logs/                # Application logs
│   └── exports/             # Analysis outputs
├── 
├── scripts/                 # Utility scripts
│   ├── check_stack.sh       # Bash smoke tests
│   ├── check_stack.ps1      # PowerShell smoke tests
│   ├── init_clickhouse.sql  # Database initialization
│   └── setup_dev.py         # Development setup
├── 
├── config/                  # Configuration files
│   ├── airflow.cfg          # Airflow configuration
│   └── logging.conf         # Logging configuration
└── 
└── .github/                 # CI/CD workflows
    └── workflows/
        └── ci.yml           # GitHub Actions CI
```

## 🔧 Development

### Common Commands
```bash
# Development setup
make init                # Install dependencies & pre-commit hooks
make dev                 # Start development environment
make test                # Run test suite
make lint                # Run code quality checks
make format              # Format code

# Data pipeline
make extract             # Run data extraction
make load                # Load data to ClickHouse
make transform           # Run dbt transformations
make dbt-run             # Run dbt models
make dbt-version         # Check dbt version and connectivity
make pipeline            # Run full ELT pipeline
make smoke               # Run health checks

# Infrastructure
make up                  # Start all services
make down                # Stop all services
make logs                # View service logs
make reset               # Reset all data & containers
```

### Testing
```bash
# Run all tests
pytest

# Run specific test categories
pytest -m unit           # Unit tests only
pytest -m integration    # Integration tests only
pytest -m "not slow"     # Skip slow tests

# With coverage
pytest --cov=connectors --cov=elt --cov-report=html
```

### Code Quality
Pre-commit hooks automatically run:
- **Black**: Code formatting
- **Ruff**: Fast linting
- **isort**: Import sorting
- **mypy**: Type checking
- **pytest**: Test execution

## 🗄️ Data Architecture

### Raw Layer (`raw` schema)
- **raw_ingestions**: Metadata about data extractions
- **raw_dawum_polls**: DAWUM polling data (JSON)
- **raw_destatis_***: Destatis datasets
- **raw_eurostat_***: Eurostat datasets
- **raw_web_scrapes**: Scraped content

### Staging Layer (`staging` schema)
- **stg_polls**: Cleaned polling data
- **stg_demographics**: Demographic indicators
- **stg_economics**: Economic indicators
- **stg_elections**: Election results

### Marts Layer (`analytics` schema)
- **dim_parties**: Political parties dimension
- **dim_institutes**: Polling institutes dimension
- **dim_geography**: Geographic dimension
- **fact_polls**: Polling facts table
- **fact_elections**: Election results facts

## 📊 Monitoring & Data Quality

### Airflow Monitoring
- Task success/failure rates
- DAG run duration
- Data freshness SLAs
- Custom metrics via Prometheus

### dbt Testing
- Not-null constraints
- Uniqueness checks
- Referential integrity
- Custom business rules
- Freshness assertions

### Alerts
- Slack notifications for failures
- Email alerts for data quality issues
- Prometheus alerts for system metrics

## 🔑 Configuration

### Environment Variables
See `.env.example` for complete configuration options:
- **Database**: ClickHouse connection settings
- **APIs**: Authentication keys for data sources
- **Storage**: MinIO/S3 credentials
- **Monitoring**: Slack webhook URLs
- **Development**: Debug flags, log levels

### Airflow Connections
Configure via Airflow UI or environment variables:
- `clickhouse_default`: ClickHouse connection
- `minio_default`: Object storage connection
- `slack_default`: Slack notifications

## 🚀 Deployment

### Production Considerations
- Use external PostgreSQL for Airflow metadata
- Configure ClickHouse cluster for high availability
- Set up proper SSL certificates
- Use Kubernetes for container orchestration
- Implement proper secrets management
- Set up automated backups

### CI/CD Pipeline
GitHub Actions workflow includes:
- Dependency installation
- Code quality checks
- Test execution
- dbt model compilation
- Docker image building
- Deployment to staging/production

## 📈 Performance Optimization

### ClickHouse Tuning
- Use appropriate table engines (MergeTree, ReplacingMergeTree)
- Optimize partition keys and order by clauses
- Configure proper compression settings
- Use materialized views for aggregations

### Pipeline Optimization
- Implement incremental data loading
- Use connection pooling for APIs
- Batch processing for large datasets
- Async/await patterns for I/O operations

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and quality checks
5. Submit a pull request

## 📝 License

MIT License - see LICENSE file for details.
