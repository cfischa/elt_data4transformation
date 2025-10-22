# BnB Data4Transformation - Political Data ELT Pipeline

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Extraction    │    │   Raw Storage   │
│                 │    │                 │    │                 │
│ • DAWUM API     │───▶│ • Connectors    │───▶│ • Filesystem    │
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
- **Async API Connectors**: DAWUM ✅, Destatis 🚧, Eurostat, GESIS ✅, SOEP ✅
- **Web Scraping**: Scrapy framework for sites without APIs
- **Object Storage**: Local filesystem for raw data (MinIO removed)
- **Analytics Warehouse**: ClickHouse for high-performance analytics
- **Transform Layer**: dbt-core with ClickHouse adapter
- **Orchestration**: Apache Airflow with TaskFlow API
- **Monitoring**: Prometheus metrics + data quality tests
- **Development**: Poetry, pre-commit, pytest, type hints

> 📋 **Connector Status**: See [Destatis Development Status](docs/DESTATIS_STATUS_TODOS.md) for detailed TODO list and implementation progress.

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- Poetry (or use provided Makefile)

### 1. Start Docker Desktop
```powershell
# Start Docker Desktop on Windows
Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe"

# Wait for Docker to be ready (30-60 seconds)
# You can verify with: docker info
```

### 2. Initialize Project
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

### 3. Start Infrastructure
Start Docker Desktop, then in your shell `conda activate bnb_data4transformation`.
```bash
# (Optional) pin a Compose project name so container names stay consistent
# PowerShell:   setx COMPOSE_PROJECT_NAME bnb_data4transformation
# macOS/Linux: export COMPOSE_PROJECT_NAME=bnb_data4transformation
# Containers will then be named like `bnb_data4transformation_clickhouse_1`.

# Start all services (ClickHouse, Airflow, etc.)
docker compose up -d

# Check service health
docker compose ps

# (Optional, first run) Initialize ClickHouse schemas
docker compose exec -T clickhouse \
  clickhouse-client --multiquery < scripts/init_clickhouse.sql

# Access services:
# - Airflow UI: http://localhost:8081 (airflow/airflow)
# - ClickHouse: http://localhost:8124
# - Jupyter: http://localhost:8888?token=admin
# - Streamlit UI: http://localhost:8501

# Alternatively, if you prefer Make targets:
# make up && make smoke
# make status
# make init-db

# If Docker reports container-name conflicts (e.g. "clickhouse already in use"),
# remove the stale standalone containers before re-running:
#   docker rm -f clickhouse postgres redis
# or reset the stack completely:
#   docker compose down --remove-orphans
```

### 4. Run Sample Pipeline
```bash
# Load sample DAWUM data
make load-local

# Extract Destatis statistical data (using new POST API since July 2025)
make extract-destatis TABLES="12411-0001,12411-0002" AREA="de" START_YEAR="2020" END_YEAR="2023"

# Test with specific credentials for large datasets
DESTATIS_USER=<REDACTED> DESTATIS_PASS=<REDACTED> make extract-destatis

# Run dbt models
make dbt-run

# Access services:
# - Airflow UI: http://localhost:8081 (airflow/airflow)
# - ClickHouse: http://localhost:8124
# - Jupyter: http://localhost:8888?token=admin
# - Streamlit UI: http://localhost:8501
```

### 5. Verify Data Loading
```bash
# Check ClickHouse data ingestion
docker exec clickhouse clickhouse-client --query "SELECT COUNT(*) as poll_count FROM raw.dawum_polls"
# Expected: ~3,500+ polling records

# View recent polls
docker exec clickhouse clickhouse-client --query "SELECT poll_id, publication_date, sample_size FROM raw.dawum_polls ORDER BY publication_date DESC LIMIT 5"

# Check all available tables
docker exec clickhouse clickhouse-client --query "SHOW TABLES FROM raw"
```

## 📊 Data Sources

### DAWUM Polling API ✅
German polling data aggregator providing real-time political polling data.

**API Status:** Fully operational (updated July 23, 2025)
- ✅ API endpoint updated from `/data.json` to root endpoint `/`
- ✅ 3,500+ polls successfully ingested into ClickHouse
- ✅ Real-time data extraction with async HTTP client
- ✅ Full data transformation pipeline operational

**Features:**
- Anonymous access (no authentication required)
- JSON format with structured polling data
- Institutes, parties, and parliament metadata
- Rate limiting and retry logic implemented

### GESIS Knowledge Graph API ✅
GESIS - Leibniz Institute for the Social Sciences provides comprehensive social science research datasets through their SPARQL endpoint.

**API Status:** Production-ready (updated August 12, 2025)
- ✅ Full pipeline operational with 9,176+ datasets
- ✅ SPARQL endpoint extraction with complete RDF metadata
- ✅ Optimized batch processing (20 datasets/batch)
- ✅ Rich metadata extraction (~40 properties per dataset)
- ✅ 100% data completeness with robust error handling

**Features:**
- SPARQL endpoint with schema:Dataset queries
- Complete RDF triple extraction for comprehensive metadata
- Batch processing to handle large dataset catalogs
- Anonymous access (no authentication required)
- Automatic retry logic for failed batches

**Usage:**
```bash
# Trigger GESIS metadata extraction DAG
airflow dags trigger fetch_gesis_metadata

# Check extracted data in ClickHouse
docker exec clickhouse clickhouse-client --query "SELECT COUNT(*) FROM raw.gesis_metadata"
# Expected: ~9,176+ research datasets
```

### SOEP Monitor API ✅
The SOEP Monitor exposes open indicator metadata and aggregated time-series derived from the German Socio-Economic Panel via a public JSON API.

**API Status:** Production-ready (updated October 21, 2025)
- ✅ Async connector with metadata + observation streaming
- ✅ Daily Airflow DAG (`fetch_soep_metadata`) populating `raw.soep_metadata`
- ✅ ClickHouse loader with ReplacingMergeTree upserts
- ✅ Unified metadata view + topic classifier integration

**Features:**
- Anonymous access to `https://monitor.soep.de/api/v1`
- Indicator detail includes topics, dimensions, units, and methodology notes
- Paginated observation endpoint with optional dimension filters
- Loader stores raw summaries + detailed payloads for downstream auditing

**Usage:**
```bash
# Trigger SOEP metadata extraction DAG
airflow dags trigger fetch_soep_metadata

# Inspect loaded indicators in ClickHouse
docker exec clickhouse clickhouse-client --query "SELECT COUNT(*) FROM raw.soep_metadata"

# Explore combined canonical metadata
docker exec clickhouse clickhouse-client --query "SELECT source, count() FROM analytics.datasets_unified GROUP BY source"
```

### GENESIS-Online (Destatis) API
The German Federal Statistical Office provides comprehensive statistical data through their GENESIS-Online REST API.


**Example API Usage:**
```bash
# Test table extraction
curl -X GET "https://www-genesis.destatis.de/genesisWS/rest/2020/data/table" \
  -H "Authorization: Basic $(echo -n 'user:pass' | base64)" \
  -G -d "name=12411-0001" \
  -d "area=de" \
  -d "format=json" \
  -d "compress=true"

# Extract population statistics
make extract-destatis TABLES="12411-0001,12411-0002" AREA="de" START_YEAR="2020" END_YEAR="2023"
```

**Supported Features:**
- Anonymous access for small tables (<50MB)
- Authenticated access for large datasets
- Auto-chunking for tables >10MB or >1M cells
- Multiple formats: JSON-stat, SDMX-JSON, CSV
- Transparent gzip decompression
- Exponential backoff retry on HTTP 5xx/429
- Rate limiting (30 requests/minute)

### Topic Classification
- Rule-based classifier (`pipeline/topic_classifier.py`) tags metadata into `analytics.dataset_topics` in ClickHouse.
- The Streamlit **Topic Classification Browser** (`streamlit_app/pages/3_Topic_Browser.py`) surfaces assignments and matched terms.
- Register new metadata adapters via `elt.adapters.metadata.register_adapter("source_name", adapter_function)` so additional sources plug into the canonical classification flow without code churn.

## 📁 Project Structure

```
├── README.md                 # This file
├── docker-compose.yml        # Infrastructure orchestration
├── .env.example              # Environment variables template
├── pyproject.toml            # Python dependencies & tools
├── Makefile                  # Common development tasks
├── 
├── dags/                     # Airflow DAG definitions
│   ├── dawum_ingest_dag.py            # DAWUM polling ingestion
│   ├── fetch_destatis_metadata_clean.py # Weekly Destatis metadata crawl
│   ├── fetch_gesis_metadata_dag.py    # Daily GESIS metadata crawl
│   ├── topic_classifier_pipeline_dag.py # Triggers classifier + ingestion on new metadata
│   ├── topic_selected_ingest_dag.py   # Topic-based dataset extraction runner
│   └── dbt_transform_dag.py           # dbt transformations
├── 
├── connectors/               # Data source connectors
│   ├── __init__.py
│   ├── base_connector.py     # Abstract base class
│   ├── dawum_connector.py    # DAWUM API client
│   ├── destatis_connector.py # GENESIS-Online POST API client (2025)
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

### GENESIS-Online API (Destatis) - Updated July 2025

**Important**: Since July 15, 2025, the SOAP/XML interface and GET methods have been discontinued. Our connector now uses the new POST-based RESTful JSON interface.

#### API Configuration
```bash
# For large datasets (>50MB) - requires registration
DESTATIS_USER=<REDACTED>
DESTATIS_PASS=<REDACTED>

# Base URL (REST v2020 with POST methods)
DESTATIS_BASE_URL=https://www-genesis.destatis.de/genesisWS/rest/2020/
```

#### Usage Examples
```bash
# Extract multiple tables with time range
make extract-destatis TABLES="12411-0001,12411-0002,21311-0001" START_YEAR="2020" END_YEAR="2023"

# Test with specific area and format
make extract-destatis TABLES="12411-0001" AREA="de" FORMAT="csv"
```

#### API Example (POST method)
```bash
curl -X POST "https://www-genesis.destatis.de/genesisWS/rest/2020/data/table" \
  -H "Content-Type: application/json" \
  -H "Authorization: Basic $(echo -n 'user:pass' | base64)" \
  -d '{
    "name": "12411-0001",
    "area": "de", 
    "format": "json",
    "compress": "true",
    "startYear": "2020",
    "endYear": "2023"
  }'
```

#### Supported Datasets
- **12411-0001**: Population by age groups and sex
- **12411-0002**: Population by citizenship  
- **21311-0001**: Live births and deaths
- **81000-0001**: Federal election results
- And thousands more statistical tables

### Raw Layer (`raw` schema)
- **raw_ingestions**: Metadata about data extractions
- **raw_dawum_polls**: DAWUM polling data (JSON) - ✅ **3,527 records verified**
- **raw_destatis_***: Destatis datasets (GENESIS-Online)
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

## 📚 Streamlit Metadata Explorer

- Start the UI: `make streamlit` (or `docker compose up -d streamlit`)
- Open your browser at `http://localhost:8501`
- Overview page shows table/column counts, top tables by size, query volume (24h), and database inventory
- Extend `streamlit_app/` with new pages to surface metadata marts, data quality results, or decision queues

## 🔑 Configuration

### Environment Variables
See `.env.example` for complete configuration options:
- **Database**: ClickHouse connection settings
- **APIs**: Authentication keys for data sources
- **Monitoring**: Slack webhook URLs
- **Development**: Debug flags, log levels

### Airflow Connections
Configure via Airflow UI or environment variables:
- `clickhouse_default`: ClickHouse connection
- `slack_default`: Slack notifications

## 🚀 Deployment

### Current Infrastructure Status (July 23, 2025)
✅ **All Systems Operational**
- **Docker Containers**: 9/9 healthy
- **Airflow**: Scheduler + Webserver running (LocalExecutor)
- **ClickHouse**: Database operational with data
- **DAWUM Pipeline**: Fully functional with 3,527 polls ingested
- **Destatis Connector**: Under development (authentication testing)

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

### Known Issues & Solutions
- **DAWUM API Endpoint Change (July 2025)**: ✅ **RESOLVED** - Updated connector to use root endpoint instead of `/data.json`
- **orjson Compilation Issues**: ✅ **RESOLVED** - Using binary wheels with `PIP_PREFER_BINARY=1`
- **Container Health**: ✅ **VERIFIED** - All 8 containers healthy and operational

### Development Workflow
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and quality checks
5. Submit a pull request

## 📝 License

MIT License - see LICENSE file for details.
