import sys
import logging
from datetime import datetime, timedelta
from typing import List

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable

sys.path.append("/opt/airflow")

from pipeline.topic_selected_ingest import main as ingest_main  # noqa: E402


default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def _parse_variable(key: str) -> List[str]:
    raw_value = Variable.get(key, default_var="")
    if not raw_value:
        return []
    return [item.strip() for item in raw_value.split(",") if item.strip()]


with DAG(
    dag_id="topic_selected_ingest",
    description="Run topic-selected dataset ingestion and persist payloads to ClickHouse.",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["topics", "clickhouse", "ingestion"],
) as dag:

    @task(task_id="run_topic_selected_ingest")
    def run_topic_selected_ingest() -> None:
        logger = logging.getLogger("topic_selected_ingest_dag")

        topics = _parse_variable("TOPIC_INGEST_TOPICS")
        sources = _parse_variable("TOPIC_INGEST_SOURCES")
        since = Variable.get("TOPIC_INGEST_SINCE", default_var=None)
        limit = Variable.get("TOPIC_INGEST_LIMIT", default_var=None)

        args: List[str] = ["--log-level", "INFO"]

        if topics:
            args.append("--topics")
            args.extend(topics)
            logger.info("Filtering topics: %s", topics)

        if sources:
            args.append("--sources")
            args.extend(sources)
            logger.info("Filtering sources: %s", sources)

        if since:
            args.extend(["--since", since])
            logger.info("Applying since filter: %s", since)

        if limit:
            args.extend(["--limit", str(limit)])
            logger.info("Applying limit: %s", limit)

        result = ingest_main(args)
        if result != 0:
            raise AirflowFailException(f"Topic-selected ingestion CLI exited with {result}")

    run_topic_selected_ingest()
