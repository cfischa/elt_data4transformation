"""
Airflow DAG for GESIS metadata extraction.
Fetches all GESIS dataset metadata and saves to meta_data/gesis_metadata.json.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
import logging
import sys
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add project root to path for imports
sys.path.append('/opt/airflow')

@dag(
    dag_id="fetch_gesis_metadata",
    description="Fetch GESIS metadata and save to meta_data/gesis_metadata.json",
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
    tags=["gesis", "metadata"],
)
def fetch_gesis_metadata_dag():
    """DAG to fetch and store GESIS dataset metadata regularly."""



    @task(task_id="fetch_10_metadata")
    def fetch_10_metadata() -> list:
        import asyncio
        from connectors.gesis_connector import GESISConnector

        async def main():
            async with GESISConnector() as connector:
                logger.info("Listing all GESIS datasets...")
                datasets = await connector.list_datasets()
                logger.info(f"Found {len(datasets)} datasets. Fetching first 10 metadata...")
                all_metadata = []
                seen_ids = set()
                for i, ds in enumerate(datasets[:10]):
                    rid = ds["id"]
                    if rid in seen_ids:
                        continue
                    seen_ids.add(rid)
                    meta = await connector.get_metadata(rid)
                    all_metadata.append(meta)
                logger.info(f"Fetched metadata for {len(all_metadata)} datasets.")
                if all_metadata:
                    logger.info(f"Sample metadata entry: {all_metadata[0]}")
                return all_metadata

        try:
            return asyncio.run(main())
        except Exception as e:
            logger.error(f"GESIS metadata fetch failed: {e}")
            raise

    @task(task_id="check_test_result", trigger_rule="all_success", multiple_outputs=True)
    def check_test_result(success: dict) -> dict:
        if not success or success.get("rows_loaded", 0) < 10:
            logger.error("Initial test load of 10 metadata failed or incomplete. Aborting full fetch.")
            return {"proceed": False}
        logger.info("Test batch loaded successfully. Proceeding to full fetch.")
        return {"proceed": True}

    @task.short_circuit(task_id="should_proceed", trigger_rule="all_success")
    def should_proceed(check: dict) -> bool:
        return check.get("proceed", False)

    @task(task_id="fetch_all_metadata", trigger_rule="all_success")
    def fetch_all_metadata() -> list:
        import asyncio
        from connectors.gesis_connector import GESISConnector

        async def main():
            async with GESISConnector() as connector:
                logger.info("Listing all GESIS datasets...")
                datasets = await connector.list_datasets()
                logger.info(f"Found {len(datasets)} datasets. Fetching all metadata...")
                all_metadata = []
                seen_ids = set()
                for i, ds in enumerate(datasets):
                    rid = ds["id"]
                    if rid in seen_ids:
                        continue
                    seen_ids.add(rid)
                    meta = await connector.get_metadata(rid)
                    all_metadata.append(meta)
                    if (i+1) % 10 == 0:
                        logger.info(f"Fetched metadata for {i+1} datasets...")
                logger.info(f"Fetched metadata for {len(all_metadata)} datasets.")
                if all_metadata:
                    logger.info(f"Sample metadata entry: {all_metadata[0]}")
                return all_metadata

        try:
            return asyncio.run(main())
        except Exception as e:
            logger.error(f"GESIS metadata fetch failed: {e}")
            raise

    @task(task_id="load_metadata_clickhouse")
    def load_metadata_clickhouse(all_metadata: list) -> dict:
        from elt.loader_clickhouse import ClickHouseLoader, ClickHouseConfig
        import os
        if not all_metadata:
            logger.warning("No metadata to load into ClickHouse.")
            return {"rows_loaded": 0, "status": "success", "message": "No data to load"}
        try:
            config = ClickHouseConfig(
                host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
                port=int(os.getenv("CLICKHOUSE_PORT", "8124")),
                username=os.getenv("CLICKHOUSE_USER", "admin"),
                password=os.getenv("CLICKHOUSE_PASSWORD", ""),
                database=os.getenv("CLICKHOUSE_DATABASE", "analytics")
            )
            with ClickHouseLoader(config) as loader:
                # You need to implement upsert_gesis_metadata in your loader
                rows_loaded = loader.upsert_gesis_metadata(all_metadata)
                logger.info(f"Successfully loaded {rows_loaded} GESIS metadata records to ClickHouse")
                return {
                    "rows_loaded": rows_loaded,
                    "status": "success",
                    "message": f"Loaded {rows_loaded} metadata records",
                    "table": "raw.gesis_metadata"
                }
        except Exception as e:
            logger.error(f"Failed to load GESIS metadata to ClickHouse: {e}")
            raise

    # Task dependencies
    test_meta = fetch_10_metadata()
    test_result = load_metadata_clickhouse(test_meta)
    check = check_test_result(test_result)
    proceed = should_proceed(check)
    all_meta = fetch_all_metadata()
    proceed >> all_meta
    load_metadata_clickhouse(all_meta)

# Instantiate the DAG
gesis_metadata_dag = fetch_gesis_metadata_dag()