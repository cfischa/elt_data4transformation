"""Airflow DAG to fetch SOEP Monitor metadata and trigger classifier."""

from datetime import datetime, timedelta
import asyncio
import logging
import os
import sys

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Ensure project modules are discoverable inside Airflow scheduler/executor
sys.path.append("/opt/airflow")


@dag(
    dag_id="fetch_soep_metadata",
    description="Fetch SOEP Monitor indicator metadata and load into ClickHouse",
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
    tags=["soep", "metadata"],
)
def fetch_soep_metadata_dag():
    """Orchestrate SOEP metadata ingestion and classifier trigger."""

    @task(task_id="fetch_soep_metadata")
    def fetch_soep_metadata() -> list:
        async def _run() -> list:
            from connectors.soep_connector import SOEPConnector

            async with SOEPConnector() as connector:
                metadata = await connector.fetch_metadata()
                logger.info("Fetched %d SOEP indicators", len(metadata))
                if metadata:
                    sample = metadata[0]
                    logger.info(
                        "Sample indicator: slug=%s title=%s",
                        sample.get("slug"),
                        sample.get("title"),
                    )
                return metadata

        try:
            return asyncio.run(_run())
        except Exception as exc:  # pragma: no cover - defensive
            logger.error("SOEP metadata fetch failed: %s", exc)
            return []

    @task(task_id="load_metadata_clickhouse")
    def load_metadata_clickhouse(metadata: list) -> dict:
        if not metadata:
            logger.warning("No SOEP metadata to load into ClickHouse")
            return {"rows_loaded": 0, "status": "empty"}

        from elt.loader_clickhouse import ClickHouseLoader, ClickHouseConfig

        config = ClickHouseConfig(
            host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            username=os.getenv("CLICKHOUSE_USER", "admin"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            database=os.getenv("CLICKHOUSE_DATABASE", "analytics"),
        )

        with ClickHouseLoader(config) as loader:
            rows_loaded = loader.upsert_soep_metadata(metadata)
            logger.info("Loaded %d SOEP metadata rows into ClickHouse", rows_loaded)
            return {
                "rows_loaded": rows_loaded,
                "status": "success" if rows_loaded else "noop",
            }

    @task.short_circuit(task_id="has_new_soep_metadata")
    def has_new_soep_metadata(load_result: dict) -> bool:
        new_rows = load_result.get("rows_loaded", 0)
        if new_rows:
            logger.info("Detected %d new SOEP metadata rows", new_rows)
            return True
        logger.info("No new SOEP metadata ingested this run")
        return False

    metadata = fetch_soep_metadata()
    load_result = load_metadata_clickhouse(metadata)
    has_new = has_new_soep_metadata(load_result)

    trigger_classifier = TriggerDagRunOperator(
        task_id="trigger_topic_classifier_pipeline",
        trigger_dag_id="topic_classifier_pipeline",
        conf={"triggered_by": "fetch_soep_metadata"},
        wait_for_completion=False,
    )

    has_new >> trigger_classifier


soep_metadata_dag = fetch_soep_metadata_dag()
