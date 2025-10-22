"""Airflow DAG for Eurostat metadata extraction."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow.decorators import dag, task
import logging
import sys
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

# Ensure project root is importable when running inside Airflow worker
sys.path.append('/opt/airflow')


@dag(
    dag_id="fetch_eurostat_metadata",
    description="Fetch Eurostat dataset catalogue and persist metadata in ClickHouse",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-team",
        "depends_on_past": False,
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["eurostat", "metadata"],
)
def fetch_eurostat_metadata_dag():
    """DAG orchestrating Eurostat metadata ingestion."""

    @task(task_id="test_api_connection")
    def test_api_connection() -> Dict[str, Any]:
        import asyncio
        from connectors.eurostat_connector import EurostatConnector

        async def main() -> Dict[str, Any]:
            async with EurostatConnector() as connector:
                page = await connector.list_datasets(page=1, page_size=5)
                return {
                    "status": "success",
                    "preview_count": len(page.get("items", [])),
                }

        return asyncio.run(main())

    @task(task_id="fetch_all_metadata", trigger_rule="all_success")
    def fetch_all_metadata() -> List[Dict[str, Any]]:
        import asyncio
        from connectors.eurostat_connector import EurostatConnector

        async def main() -> List[Dict[str, Any]]:
            async with EurostatConnector() as connector:
                page = 1
                page_size = 100
                collected: List[Dict[str, Any]] = []
                while True:
                    payload = await connector.list_datasets(page=page, page_size=page_size)
                    items = payload.get("items", [])
                    if not items:
                        break
                    for entry in items:
                        metadata = await connector.get_dataset_metadata(entry["code"])
                        collected.append(
                            {
                                "dataset_code": entry.get("code"),
                                "title": entry.get("title"),
                                "description": entry.get("description"),
                                "last_update": metadata.get("last_update") or entry.get("last_update"),
                                "keywords": entry.get("keywords"),
                                "themes": entry.get("themes"),
                                "dimensions": metadata.get("dimension", {}).get("id") if isinstance(metadata.get("dimension"), dict) else entry.get("dimensions"),
                                "values_count": entry.get("values_count"),
                                "raw": metadata.get("raw") or metadata,
                                "fetched_at": datetime.utcnow(),
                            }
                        )
                    if len(items) < page_size:
                        break
                    page += 1
                LOGGER.info("Fetched %s Eurostat metadata entries", len(collected))
                return collected

        return asyncio.run(main())

    @task(task_id="load_metadata_clickhouse", trigger_rule="all_success")
    def load_metadata_clickhouse(records: List[Dict[str, Any]]) -> Dict[str, Any]:
        from elt.loader_clickhouse import ClickHouseLoader, ClickHouseConfig

        if not records:
            LOGGER.warning("No Eurostat metadata to load.")
            return {"rows_loaded": 0, "status": "empty"}

        config = ClickHouseConfig(
            host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            username=os.getenv("CLICKHOUSE_USER", "admin"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            database=os.getenv("CLICKHOUSE_DATABASE", "analytics"),
        )

        with ClickHouseLoader(config) as loader:
            loaded = loader.upsert_eurostat_metadata(records)
            LOGGER.info("Loaded %s Eurostat metadata rows into ClickHouse", loaded)
            return {"rows_loaded": loaded, "status": "success"}

    @task(task_id="validate_ingestion", trigger_rule="all_success")
    def validate_ingestion(load_result: Dict[str, Any]) -> Dict[str, Any]:
        if load_result.get("status") != "success":
            return {"status": "skipped"}

        from elt.loader_clickhouse import ClickHouseLoader, ClickHouseConfig

        config = ClickHouseConfig(
            host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            username=os.getenv("CLICKHOUSE_USER", "admin"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            database=os.getenv("CLICKHOUSE_DATABASE", "analytics"),
        )

        with ClickHouseLoader(config) as loader:
            total = loader.client.query("SELECT COUNT(*) FROM raw.eurostat_metadata").result_rows[0][0]
            latest = loader.client.query(
                "SELECT dataset_code, title FROM raw.eurostat_metadata ORDER BY fetched_at DESC LIMIT 3"
            ).result_rows
            LOGGER.info("Eurostat metadata table now has %s rows", total)
            return {
                "status": "success",
                "total_rows": total,
                "sample": latest,
            }

    preview = test_api_connection()
    metadata = fetch_all_metadata()
    preview >> metadata
    load = load_metadata_clickhouse(metadata)
    validate_ingestion(load)


dag = fetch_eurostat_metadata_dag()
