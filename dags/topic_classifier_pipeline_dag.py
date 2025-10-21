"""Airflow DAG for running the rule-based topic classifier pipeline."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import List, Optional

from airflow import DAG
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable

try:
    from dags.utils.module_loader import load_module_main
except ModuleNotFoundError:
    import sys
    from pathlib import Path

    current_dir = Path(__file__).resolve().parent
    utils_path = current_dir / "utils"
    if utils_path.is_dir() and str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    from utils.module_loader import load_module_main

classifier_main = load_module_main(__file__, "pipeline.topic_classifier")


def _parse_list_variable(key: str) -> List[str]:
    raw_value = Variable.get(key, default_var="")
    if not raw_value:
        return []
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def _parse_bool_variable(key: str) -> bool:
    raw_value = Variable.get(key, default_var="").strip().lower()
    if not raw_value:
        return False
    return raw_value in {"1", "true", "t", "yes", "y"}


def _parse_int_variable(key: str) -> Optional[int]:
    raw_value = Variable.get(key, default_var="").strip()
    if not raw_value:
        return None
    try:
        return int(raw_value)
    except ValueError as exc:
        raise AirflowFailException(f"Variable {key!r} must be an integer, got: {raw_value!r}") from exc


default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


with DAG(
    dag_id="topic_classifier_pipeline",
    description="Run topic classifier and persist assignments to ClickHouse.",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["topics", "clickhouse", "classification"],
) as dag:

    @task(task_id="run_topic_classifier")
    def run_topic_classifier() -> None:
        logger = logging.getLogger("topic_classifier_pipeline_dag")

        log_level = Variable.get("TOPIC_CLASSIFIER_LOG_LEVEL", default_var="INFO").strip() or "INFO"
        args: List[str] = ["--log-level", log_level.upper()]

        taxonomy_path = Variable.get("TOPIC_CLASSIFIER_TAXONOMY", default_var="").strip()
        if taxonomy_path:
            args.extend(["--taxonomy", taxonomy_path])
            logger.info("Using taxonomy file: %s", taxonomy_path)

        sources = _parse_list_variable("TOPIC_CLASSIFIER_SOURCES")
        if sources:
            args.append("--sources")
            args.extend(sources)
            logger.info("Restricting classifier to sources: %s", sources)

        since = Variable.get("TOPIC_CLASSIFIER_SINCE", default_var="").strip()
        if since:
            args.extend(["--since", since])
            logger.info("Processing datasets updated since: %s", since)

        limit_value = _parse_int_variable("TOPIC_CLASSIFIER_LIMIT")
        if limit_value is not None:
            args.extend(["--limit", str(limit_value)])
            logger.info("Limiting classifier to %d datasets", limit_value)

        if _parse_bool_variable("TOPIC_CLASSIFIER_DRY_RUN"):
            args.append("--dry-run")
            logger.info("Running classifier in dry-run mode")

        logger.info("Invoking topic classifier CLI with args: %s", args)
        result = classifier_main(args)
        if result != 0:
            raise AirflowFailException(f"Topic classifier CLI exited with {result}")

    run_topic_classifier()
