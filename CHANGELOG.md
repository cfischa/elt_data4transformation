# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **Critical**: Fixed indentation error in `tests/test_clickhouse_connection.py` that caused Pylance compilation errors
- **Async Operations**: Replaced all synchronous `open()` calls with `aiofiles.open()` in async functions within DAG files
- **F-strings**: Fixed empty f-string replacement field in `dags/extract_api_dag.py` notification formatting
- **Docker Security**: Removed hardcoded secrets from `docker-compose.yml`, replaced with environment variable placeholders
- **Code Quality**: Reduced cognitive complexity in `tests/test_api_manual.py` from 62 to <15 by:
  - Breaking down monolithic function into smaller, focused methods
  - Creating dedicated classes for configuration and result handling
  - Implementing separation of concerns with dedicated test methods
- **TODOs**: Completed or documented all TODO comments with concrete implementation plans
- **Unused Variables**: Fixed unused variable warnings in test files
- **Environment Parsing**: Added robust parsing for environment variables containing comments

### Added
- **Exception Handling**: Created custom exception classes in `exceptions.py` for better error categorization:
  - `BnBDataException` (base exception)
  - `ConnectorException`, `AuthenticationException`, `APIRateLimitException`
  - `DataValidationException`, `LoaderException`, `ConfigurationException`, `PipelineException`
- **Configuration**: Enhanced `.env.example` with comprehensive documentation and security placeholders
- **Dependencies**: Ensured `aiofiles` is properly declared in requirements for async file operations

### Changed
- **Data Serialization**: Switched from standard `json` module to `orjson` for better performance in async contexts
- **Error Handling**: Improved error messages and logging throughout the codebase
- **Code Organization**: Refactored complex test functions into maintainable, single-responsibility components

### Refactored
- **tests/test_api_manual.py**: Complete rewrite with class-based architecture
  - `APITestConfig` class for configuration management
  - `APITestResult` class for result handling  
  - `DestatisAPITester` class with focused test methods
  - Eliminated nested loops and complex conditional logic

### Security
- **Secrets Management**: Removed all hardcoded passwords and API keys from configuration files
- **Environment Variables**: Added secure placeholder system for all sensitive configuration

### Documentation
- **Environment Setup**: Updated `.env.example` with clear instructions for all required variables
- **Security Guidelines**: Added comments for secure key generation methods

## [0.1.0] - 2024-01-XX

### Added
- Initial release with basic ELT pipeline functionality
- Connector implementations for DAWUM, Destatis, Eurostat, GESIS, and SOEP APIs
- ClickHouse integration for data storage
- Airflow DAGs for orchestration
- Docker Compose setup for development environment
