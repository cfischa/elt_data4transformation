"""
Test configuration for DAWUM pipeline tests.
Configures pytest markers and test environment.
"""

import pytest
import os


def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests (slow, requires services)"
    )
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests (fast, mocked)"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to handle markers."""
    for item in items:
        # Mark integration tests
        if "integration" in item.nodeid:
            item.add_marker(pytest.mark.integration)
        else:
            item.add_marker(pytest.mark.unit)


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Setup test environment variables."""
    # Set default test environment variables
    test_env = {
        "CLICKHOUSE_HOST": "localhost",
        "CLICKHOUSE_PORT": "8124",
        "CLICKHOUSE_USER": "admin",
        "CLICKHOUSE_PASSWORD": "asjrh25423sfa#+43qw56j",
        "CLICKHOUSE_DATABASE": "analytics",
        "DAWUM_BASE_URL": "https://api.dawum.de",
        "LOG_LEVEL": "INFO"
    }
    
    for key, value in test_env.items():
        if key not in os.environ:
            os.environ[key] = value


@pytest.fixture
def mock_env_vars():
    """Fixture to temporarily set environment variables for tests."""
    original_env = os.environ.copy()
    
    def _set_env(**kwargs):
        for key, value in kwargs.items():
            os.environ[key] = str(value)
    
    yield _set_env
    
    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)
