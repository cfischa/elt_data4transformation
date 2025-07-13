"""
Smoke tests for ELT stack connectivity and health checks.
"""
import os
import pytest
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@pytest.fixture
def http_session():
    """Create HTTP session with retry strategy."""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


@pytest.fixture
def airflow_url():
    """Get Airflow URL from environment."""
    return os.getenv("AIRFLOW_URL", "http://localhost:8080")


@pytest.fixture
def clickhouse_config():
    """Get ClickHouse configuration from environment."""
    return {
        "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
        "port": int(os.getenv("CLICKHOUSE_PORT", "8123")),
        "user": os.getenv("CLICKHOUSE_USER", "default"),
        "password": os.getenv("CLICKHOUSE_PASSWORD", ""),
    }


@pytest.mark.smoke
class TestStackConnectivity:
    """Smoke tests for stack connectivity."""

    def test_airflow_health(self, http_session, airflow_url):
        """Test Airflow webserver health endpoint."""
        health_url = f"{airflow_url}/health"
        
        response = http_session.get(health_url, timeout=30)
        assert response.status_code == 200
        
        health_data = response.json()
        assert "metadatabase" in health_data
        assert health_data["metadatabase"]["status"] == "healthy"

    def test_airflow_webserver_reachable(self, http_session, airflow_url):
        """Test Airflow webserver is reachable."""
        response = http_session.get(airflow_url, timeout=30, allow_redirects=True)
        assert response.status_code == 200
        assert "Airflow" in response.text

    @pytest.mark.parametrize(
        "query,expected_result",
        [
            ("SELECT 1", "1"),
            ("SELECT 'test'", "test"),
            ("SELECT now() > '2020-01-01'", "1"),
        ]
    )
    def test_clickhouse_queries(self, http_session, clickhouse_config, query, expected_result):
        """Test ClickHouse responds to basic queries."""
        url = f"http://{clickhouse_config['host']}:{clickhouse_config['port']}/"
        
        params = {"query": query}
        auth = None
        if clickhouse_config["password"]:
            auth = (clickhouse_config["user"], clickhouse_config["password"])
        
        response = http_session.get(url, params=params, auth=auth, timeout=30)
        assert response.status_code == 200
        assert response.text.strip() == expected_result

    def test_clickhouse_system_tables(self, http_session, clickhouse_config):
        """Test ClickHouse system tables are accessible."""
        url = f"http://{clickhouse_config['host']}:{clickhouse_config['port']}/"
        
        query = "SELECT count() > 0 FROM system.databases WHERE name = 'system'"
        params = {"query": query}
        auth = None
        if clickhouse_config["password"]:
            auth = (clickhouse_config["user"], clickhouse_config["password"])
        
        response = http_session.get(url, params=params, auth=auth, timeout=30)
        assert response.status_code == 200
        assert response.text.strip() == "1"

    def test_clickhouse_database_access(self, http_session, clickhouse_config):
        """Test access to configured database."""
        url = f"http://{clickhouse_config['host']}:{clickhouse_config['port']}/"
        
        # Get current database
        query = "SELECT currentDatabase()"
        params = {"query": query}
        auth = None
        if clickhouse_config["password"]:
            auth = (clickhouse_config["user"], clickhouse_config["password"])
        
        response = http_session.get(url, params=params, auth=auth, timeout=30)
        assert response.status_code == 200
        
        database = response.text.strip()
        assert len(database) > 0
        assert database != "Unknown database"


@pytest.mark.smoke
class TestServiceEndpoints:
    """Test service endpoints are responding."""

    @pytest.mark.parametrize(
        "service_name,url_path,expected_status",
        [
            ("airflow_health", "http://localhost:8080/health", 200),
            ("airflow_api", "http://localhost:8080/api/v1/health", 200),
            ("clickhouse_ping", "http://localhost:8123/ping", 200),
        ]
    )
    def test_service_endpoints(self, http_session, service_name, url_path, expected_status):
        """Test various service endpoints respond correctly."""
        response = http_session.get(url_path, timeout=30)
        assert response.status_code == expected_status, f"{service_name} endpoint failed"


@pytest.mark.smoke
@pytest.mark.integration
class TestDataPipeline:
    """Integration tests for data pipeline components."""

    def test_clickhouse_create_temp_table(self, http_session, clickhouse_config):
        """Test ClickHouse can create and query temporary tables."""
        url = f"http://{clickhouse_config['host']}:{clickhouse_config['port']}/"
        auth = None
        if clickhouse_config["password"]:
            auth = (clickhouse_config["user"], clickhouse_config["password"])

        # Create temporary table
        create_query = """
        CREATE TEMPORARY TABLE test_table (
            id UInt32,
            name String,
            created_at DateTime DEFAULT now()
        )
        """
        
        response = http_session.post(
            url,
            data=create_query,
            auth=auth,
            timeout=30
        )
        assert response.status_code == 200

        # Insert test data
        insert_query = "INSERT INTO test_table (id, name) VALUES (1, 'test')"
        response = http_session.post(
            url,
            data=insert_query,
            auth=auth,
            timeout=30
        )
        assert response.status_code == 200

        # Query test data
        select_params = {"query": "SELECT count() FROM test_table"}
        response = http_session.get(
            url,
            params=select_params,
            auth=auth,
            timeout=30
        )
        assert response.status_code == 200
        assert response.text.strip() == "1"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "smoke"])
