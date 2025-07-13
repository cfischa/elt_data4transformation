# Common development tasks for BnB Data4Transformation

.PHONY: help init dev test lint format up down logs reset extract load transform pipeline status init-db dbt-run smoke

# Default target
help:
	@echo "BnB Data4Transformation - Development Commands"
	@echo ""
	@echo "Setup:"
	@echo "  init         Install dependencies and pre-commit hooks"
	@echo "  dev          Start development environment"
	@echo ""
	@echo "Code Quality:"
	@echo "  test         Run test suite"
	@echo "  lint         Run linting checks"
	@echo "  format       Format code with black and isort"
	@echo "  smoke        Run container health checks"
	@echo ""
	@echo "Infrastructure:"
	@echo "  up           Start all services (ClickHouse, Airflow, MinIO, etc.)"
	@echo "  down         Stop all services"
	@echo "  logs         View service logs"
	@echo "  status       Check service health"
	@echo "  reset        Reset all data and containers"
	@echo "  init-db      Initialize ClickHouse schemas"
	@echo ""
	@echo "Data Pipeline:"
	@echo "  extract      Run data extraction"
	@echo "  load         Load data to ClickHouse"
	@echo "  transform    Run dbt transformations"
	@echo "  dbt-run      Run dbt models"
	@echo "  pipeline     Run full ELT pipeline"
	@echo "  load-local   Load sample DAWUM data"

# Development Setup
init:
	@echo "🚀 Initializing development environment..."
	@if not exist ".env" copy .env.example .env
	@poetry install --all-extras
	@poetry run pre-commit install
	@echo "✅ Development environment initialized"

dev: init up
	@echo "🔧 Starting development environment..."
	@echo "Access services at:"
	@echo "  - Airflow UI: http://localhost:8080 (airflow/airflow)"
	@echo "  - ClickHouse: http://localhost:8123"
	@echo "  - MinIO: http://localhost:9001 (minioadmin/minioadmin)"
	@echo "  - Jupyter: http://localhost:8888?token=admin"

# Code Quality
test:
	@echo "🧪 Running test suite..."
	@poetry run pytest -v

test-unit:
	@echo "🧪 Running unit tests..."
	@poetry run pytest -m unit -v

test-integration:
	@echo "🧪 Running integration tests..."
	@poetry run pytest -m integration -v

test-coverage:
	@echo "🧪 Running tests with coverage..."
	@poetry run pytest --cov=connectors --cov=elt --cov=scraping --cov-report=html --cov-report=term-missing

smoke:  ## run container health checks
	@echo "🔍 Running smoke tests..."
	@./scripts/check_stack.sh

lint:
	@echo "🔍 Running linting checks..."
	@poetry run ruff check .
	@poetry run black --check .
	@poetry run isort --check-only .
	@poetry run mypy connectors elt scraping

format:
	@echo "🎨 Formatting code..."
	@poetry run black .
	@poetry run isort .
	@poetry run ruff --fix .

# Infrastructure
up:
	@echo "🐳 Starting all services..."
	@docker-compose up -d
	@echo "⏳ Waiting for services to start..."
	@timeout /t 30 /nobreak > nul
	@echo "✅ Services started"

down:
	@echo "🐳 Stopping all services..."
	@docker-compose down

logs:
	@echo "📋 Viewing service logs..."
	@docker-compose logs -f

status:
	@echo "📊 Checking service health..."
	@docker-compose ps

reset:
	@echo "🗑️ Resetting all data and containers..."
	@docker-compose down -v --remove-orphans
	@docker-compose build --no-cache
	@echo "✅ Reset complete"

init-db:
	@echo "🗃️ Initializing ClickHouse schemas..."
	@docker-compose exec -T clickhouse clickhouse-client --multiquery < scripts/init_clickhouse.sql
	@echo "✅ Database initialized"

# Data Pipeline
extract:
	@echo "📥 Running data extraction..."
	@poetry run python -m connectors.dawum_connector

load:
	@echo "📤 Loading data to ClickHouse..."
	@poetry run python -m elt.loader_clickhouse

transform:
	@echo "🔄 Running dbt transformations..."
	@docker-compose exec dbt dbt run --profiles-dir /root/.dbt

dbt-run:
	@echo "🔄 Running dbt models..."
	@docker-compose exec dbt dbt run --profiles-dir /root/.dbt

dbt-test:
	@echo "🧪 Running dbt tests..."
	@docker-compose exec dbt dbt test --profiles-dir /root/.dbt

dbt-docs:
	@echo "📖 Generating dbt documentation..."
	@docker-compose exec dbt dbt docs generate --profiles-dir /root/.dbt
	@docker-compose exec dbt dbt docs serve --profiles-dir /root/.dbt --port 8081

pipeline: extract load transform
	@echo "🎯 Full ELT pipeline completed"

load-local:
	@echo "📥 Loading sample DAWUM data..."
	@poetry run python -c "from data_acquisition.connectors.dawum_connector import DawumConnector; import asyncio; asyncio.run(DawumConnector().fetch_and_store_polls())"
	@echo "✅ Sample data loaded"

# Jupyter
jupyter:
	@echo "📊 Starting Jupyter notebook..."
	@docker-compose exec jupyter jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root

# Airflow
airflow-reset:
	@echo "🔄 Resetting Airflow database..."
	@docker-compose exec airflow-webserver airflow db reset --yes

airflow-user:
	@echo "👤 Creating Airflow admin user..."
	@docker-compose exec airflow-webserver airflow users create \
		--username admin \
		--firstname Admin \
		--lastname User \
		--role Admin \
		--email admin@example.com \
		--password admin

# Monitoring
monitor:
	@echo "📊 Opening monitoring dashboard..."
	@start http://localhost:8080  # Airflow
	@start http://localhost:8123  # ClickHouse
	@start http://localhost:9001  # MinIO

# Cleanup
clean:
	@echo "🧹 Cleaning up temporary files..."
	@if exist "htmlcov" rmdir /s /q htmlcov
	@if exist ".coverage" del .coverage
	@if exist ".pytest_cache" rmdir /s /q .pytest_cache
	@if exist "**/__pycache__" rmdir /s /q "**/__pycache__"

# Production
build:
	@echo "🏗️ Building production images..."
	@docker-compose -f docker-compose.prod.yml build

deploy:
	@echo "🚀 Deploying to production..."
	@docker-compose -f docker-compose.prod.yml up -d
