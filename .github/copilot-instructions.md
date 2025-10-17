# GitHub Copilot Instructions for BnB Data4Transformation

You are an AI coding assistant for the BnB Data4Transformation project. Follow these guidelines to ensure code quality and consistency across the project.


## Project Overview
This is a political data ELT (Extract, Load, Transform) pipeline project that:
- Extracts data from various political APIs and web sources
- Loads data into ClickHouse database
- Transforms data using dbt
- Orchestrates workflows with Apache Airflow

## Project Structure
- `connectors/` - API connectors for political data sources
- `dags/` - Airflow DAGs for workflow orchestration
- `elt/` - ClickHouse data loading and ELT processes
- `scraping/` - Web scraping modules for political websites
- `dbt_project/` - dbt transformations and data models
- `docs/` - Project documentation
- `tests/` - Test suites

## Coding Conventions
- Use Python 3.x for all scripts
- Follow PEP 8 style guidelines
- Use type hints where applicable
- Prefer async/await for I/O operations
- Use structured logging with appropriate log levels

## Key Technologies
- **Database**: ClickHouse
- **Orchestration**: Apache Airflow
- **Transformations**: dbt
- **Web Scraping**: BeautifulSoup, Selenium
- **HTTP Requests**: aiohttp, requests
- **Data Processing**: pandas, polars

## File Naming Patterns
- DAGs: `dag_*.py`
- Connectors: `*_connector.py`
- Models: `*.sql` in dbt_project/models/
- Tests: `test_*.py`

## Context-Specific Guidelines
- Always include error handling for API rate limits
- Use environment variables for sensitive configuration
- Include proper SQL escaping for ClickHouse queries
- Follow Airflow best practices for DAG design

## Dependencies
- Refer to `DEPENDENCIES.md` for package management
- Use virtual environments for development
- Keep requirements files updated
- .env contains all environment variables

## Readme and Documentation
- Update `README.md` with project overview and every new feature labeled as working or in progess 
- dont use any symbols 

## Testing
- Always within the `tests/` directory
- Write unit tests for all connectors and transformations
- Include integration tests for database operations
- Test DAGs with Airflow's testing utilities