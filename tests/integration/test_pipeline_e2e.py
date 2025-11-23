import pytest
import requests
import time
import os
from clickhouse_connect import get_client

# Configuration matching your docker-compose
AIRFLOW_API_URL = "http://localhost:8081/api/v1"
AUTH = ("admin", "admin")  # Default airflow creds

@pytest.mark.integration
def test_dawum_dag_e2e():
    """
    1. Trigger the 'dawum_ingest' DAG
    2. Poll for completion
    3. Verify data landed in ClickHouse
    """
    
    # 1. Trigger DAG
    dag_id = "dawum_ingest"
    try:
        # Check if DAG exists and is unpaused
        resp = requests.patch(
            f"{AIRFLOW_API_URL}/dags/{dag_id}",
            json={"is_paused": False},
            auth=AUTH
        )
        assert resp.status_code == 200, f"Failed to unpause DAG: {resp.text}"

        # Trigger DAG run
        resp = requests.post(
            f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns",
            json={"conf": {}},
            auth=AUTH
        )
        assert resp.status_code == 200, f"Failed to trigger DAG: {resp.text}"
        dag_run_id = resp.json()["dag_run_id"]
        print(f"🚀 Triggered DAG run: {dag_run_id}")
    except requests.exceptions.ConnectionError:
        pytest.fail("Could not connect to Airflow API. Is the Airflow webserver running?")

    # 2. Wait for completion (Polling)
    state = "running"
    max_retries = 60  # Wait up to 2 minutes
    for i in range(max_retries):
        time.sleep(2)
        status_resp = requests.get(
            f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns/{dag_run_id}",
            auth=AUTH
        )
        state = status_resp.json()["state"]
        print(f"⏳ DAG state: {state} ({i+1}/{max_retries})")
        if state in ["success", "failed"]:
            break
    
    assert state == "success", f"DAG run failed with state: {state}"

    # 3. Verify Data in ClickHouse
    # Connect to internal or external port depending on where test runs
    # Tests running inside docker (CI) use host 'clickhouse' and port 8123
    # Tests running locally use localhost and port 8124
    
    # We check environment variables first
    host = os.getenv("CLICKHOUSE_HOST", "localhost")
    port = int(os.getenv("CLICKHOUSE_PORT", "8124"))
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    
    print(f"Connecting to ClickHouse at {host}:{port} as {user}")
    
    client = get_client(host=host, port=port, username=user, password=password)
    
    # Check raw table
    try:
        result = client.query("SELECT count() FROM raw.dawum_polls")
        row_count = result.result_rows[0][0]
        
        print(f"✅ Found {row_count} rows in raw.dawum_polls")
        # Depending on if data was already there, count should be > 0
        # If this is a fresh test env, it should be exactly the number of polls fetched
        assert row_count > 0, "No data found in ClickHouse after DAG run"
    except Exception as e:
        pytest.fail(f"Failed to query ClickHouse: {e}")


