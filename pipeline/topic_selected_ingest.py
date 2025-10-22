"""
CLI scaffold for ingesting topic-selected datasets into ClickHouse.

Workflow outline:
1. Read classified dataset catalog (source/topic pairs).
2. Group selections per source.
3. Dispatch to source-specific extractors (stubs for now).
4. Prepare data for persistence in `raw.topic_selected_payloads`.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import json

try:
    import orjson  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    orjson = None

from connectors.destatis_connector import DestatisConnector, DestatisConfig
from connectors.eurostat_connector import EurostatConnector
from connectors.gesis_connector import GESISConnector
from connectors.soep_connector import SOEPConnector
from elt.loader_clickhouse import ClickHouseLoader

LOGGER = logging.getLogger("topic_selected_ingest")
DEFAULT_THRESHOLD = 0.5


@dataclass
class Selection:
    """Represents a single dataset/topic selection awaiting extraction."""

    source: str
    dataset_id: str
    topic_id: str
    score: float
    decided_at: datetime
    classifier_version: str
    metadata_hash: Optional[str]
    latest_update: Optional[datetime]
    payload: Dict[str, Any]


@dataclass
class IngestionRecord:
    """Payload prepared for persistence."""

    selection: Selection
    payload_format: str
    payload: str
    records_count: int
    ingestion_status: str
    ingestion_started_at: datetime
    ingestion_completed_at: datetime
    error_message: Optional[str] = None


def _to_json_text(payload: Any) -> str:
    """Serialize payload to pretty JSON string."""
    if orjson is not None:
        return orjson.dumps(payload, option=orjson.OPT_INDENT_2).decode("utf-8")
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _load_selection_query() -> str:
    """Load the base selection query from disk."""
    path = Path("sql/select_topic_dataset_catalog.sql")
    if not path.exists():
        raise FileNotFoundError(f"Selection query file missing: {path}")
    text = path.read_text(encoding="utf-8").strip()
    # remove trailing semicolon to allow wrapping conditions
    if text.endswith(";"):
        text = text[:-1]
    return text


def fetch_selections(
    loader: ClickHouseLoader,
    sources: Optional[Iterable[str]],
    topics: Optional[Iterable[str]],
    since: Optional[str],
    limit: Optional[int],
    min_score: float = DEFAULT_THRESHOLD,
) -> List[Selection]:
    """Query ClickHouse for topic-selected datasets."""
    base_query = _load_selection_query()
    query = f"""
        WITH catalog AS (
            {base_query}
        )
        SELECT *
        FROM catalog
        WHERE score >= %(min_score)s
    """

    params: Dict[str, Any] = {"min_score": min_score}
    if sources:
        query += " AND source IN %(sources)s"
        params["sources"] = tuple({src.lower() for src in sources})

    if topics:
        query += " AND topic_id IN %(topics)s"
        params["topics"] = tuple(topics)

    if since:
        query += " AND decided_at >= parseDateTimeBestEffort(%(since)s)"
        params["since"] = since

    query += " ORDER BY source, dataset_id, topic_id"
    if limit is not None:
        query += " LIMIT %(limit)s"
        params["limit"] = int(limit)

    result = loader.client.query(query, parameters=params)
    selections: List[Selection] = []
    for row in result.named_results():
        selections.append(
            Selection(
                source=str(row["source"]),
                dataset_id=str(row["dataset_id"]),
                topic_id=str(row["topic_id"]),
                score=float(row["score"]),
                decided_at=row["decided_at"],
                classifier_version=str(row["classifier_version"]),
                metadata_hash=row.get("metadata_hash"),
                latest_update=row.get("latest_update"),
                payload={
                    "title": row.get("title"),
                    "description": row.get("description"),
                    "keywords": row.get("keywords"),
                    "variables": row.get("variables"),
                    "raw_metadata": row.get("raw_metadata"),
                },
            )
        )
    return selections


def ensure_payload_table(loader: ClickHouseLoader) -> None:
    """Apply DDL for topic-selected payload storage."""
    ddl_path = Path("sql/create_topic_selected_payloads_table.sql")
    if not ddl_path.exists():
        LOGGER.warning("DDL file missing: %s", ddl_path)
        return
    statements = [
        stmt.strip()
        for stmt in ddl_path.read_text(encoding="utf-8").split(";")
        if stmt.strip()
    ]
    for statement in statements:
        loader.client.command(statement)


def bucket_by_source(selections: Sequence[Selection]) -> Dict[str, List[Selection]]:
    """Group selections by metadata source."""
    buckets: Dict[str, List[Selection]] = defaultdict(list)
    for item in selections:
        buckets[item.source].append(item)
    return buckets


async def _extract_destatis(selections: Sequence[Selection]) -> List[IngestionRecord]:
    """Extract datasets for Destatis selections."""
    records: List[IngestionRecord] = []
    if not selections:
        return records

    cfg = DestatisConfig()
    async with DestatisConnector(cfg) as connector:
        for selection in selections:
            start_ts = datetime.utcnow()
            payload_text = ""
            record_count = 0
            status = "success"
            error_message: Optional[str] = None
            payload_format = "text"

            try:
                cube_path = await connector.fetch_cube(selection.dataset_id)
                if not cube_path.exists():
                    raise FileNotFoundError(f"Cube file missing: {cube_path}")
                payload_text = await asyncio.to_thread(
                    cube_path.read_text, "utf-8", "replace"
                )
                record_count = max(payload_text.count("\n"), 0)
                try:
                    await asyncio.to_thread(cube_path.unlink, True)
                except Exception:  # pragma: no cover - best effort cleanup
                    LOGGER.debug("Temporary file %s could not be removed", cube_path)
            except Exception as cube_exc:  # pragma: no cover - defensive
                LOGGER.warning(
                    "Cube extraction failed for %s (%s); trying table fallback",
                    selection.dataset_id,
                    cube_exc,
                )
                try:
                    table_path = await connector.fetch_table(
                        selection.dataset_id, fmt="csv"
                    )
                    if not table_path.exists():
                        raise FileNotFoundError(f"Table file missing: {table_path}")
                    payload_text = await asyncio.to_thread(
                        table_path.read_text, "utf-8", "replace"
                    )
                    payload_format = "csv"
                    record_count = max(payload_text.count("\n") - 1, 0)
                    try:
                        await asyncio.to_thread(table_path.unlink, True)
                    except Exception:
                        LOGGER.debug(
                            "Temporary file %s could not be removed", table_path
                        )
                except Exception as exc:  # pragma: no cover - defensive
                    status = "failed"
                    error_message = str(exc)
                    payload_text = error_message
                    LOGGER.exception(
                        "Destatis extraction failed for %s: %s",
                        selection.dataset_id,
                        exc,
                    )

            completed_ts = datetime.utcnow()
            records.append(
                IngestionRecord(
                    selection=selection,
                    payload_format=payload_format,
                    payload=payload_text,
                    records_count=record_count,
                    ingestion_status=status,
                    ingestion_started_at=start_ts,
                    ingestion_completed_at=completed_ts,
                    error_message=error_message,
                )
            )
    return records


async def _extract_gesis(selections: Sequence[Selection]) -> List[IngestionRecord]:
    """Extract metadata payloads for GESIS selections."""
    records: List[IngestionRecord] = []
    if not selections:
        return records

    async with GESISConnector() as connector:
        for selection in selections:
            start_ts = datetime.utcnow()
            payload_text = ""
            record_count = 0
            status = "success"
            error_message: Optional[str] = None

            try:
                metadata = await connector.get_metadata(selection.dataset_id)
                payload_text = _to_json_text(metadata)
                record_count = (
                    len(metadata.get("properties", {}))
                    if isinstance(metadata, dict)
                    else 0
                )
            except Exception as exc:  # pragma: no cover - defensive
                status = "failed"
                error_message = str(exc)
                payload_text = error_message
                LOGGER.exception(
                    "GESIS extraction failed for %s: %s", selection.dataset_id, exc
                )

            completed_ts = datetime.utcnow()
            records.append(
                IngestionRecord(
                    selection=selection,
                    payload_format="json",
                    payload=payload_text,
                    records_count=record_count,
                    ingestion_status=status,
                    ingestion_started_at=start_ts,
                    ingestion_completed_at=completed_ts,
                    error_message=error_message,
                )
            )
    return records


async def _extract_soep(selections: Sequence[Selection]) -> List[IngestionRecord]:
    """Extract SOEP indicator observations for selections."""

    records: List[IngestionRecord] = []
    if not selections:
        return records

    async with SOEPConnector() as connector:
        for selection in selections:
            start_ts = datetime.utcnow()
            payload_text = ""
            record_count = 0
            status = "success"
            error_message: Optional[str] = None

            try:
                async for payload in connector.fetch_data(
                    datasets=[selection.dataset_id]
                ):
                    payload_text = _to_json_text(payload)
                    record_count = len(payload.get("observations", []))
                    break
                if not payload_text:
                    payload_text = _to_json_text(
                        {
                            "source": "soep",
                            "dataset": selection.dataset_id,
                            "observations": [],
                        }
                    )
            except Exception as exc:  # pragma: no cover - defensive
                status = "failed"
                error_message = str(exc)
                payload_text = error_message
                LOGGER.exception(
                    "SOEP extraction failed for %s: %s", selection.dataset_id, exc
                )

            completed_ts = datetime.utcnow()
            records.append(
                IngestionRecord(
                    selection=selection,
                    payload_format="json",
                    payload=payload_text,
                    records_count=record_count,
                    ingestion_status=status,
                    ingestion_started_at=start_ts,
                    ingestion_completed_at=completed_ts,
                    error_message=error_message,
                )
            )

    return records


async def _extract_eurostat(selections: Sequence[Selection]) -> List[IngestionRecord]:
    """Extract Eurostat datasets as JSON payloads."""

    records: List[IngestionRecord] = []
    if not selections:
        return records

    async with EurostatConnector() as connector:
        for selection in selections:
            start_ts = datetime.utcnow()
            payload_text = ""
            record_count = 0
            status = "success"
            error_message: Optional[str] = None

            try:
                result = await connector.fetch_dataset(selection.dataset_id)
                payload_text = _to_json_text(
                    {
                        "dataset_id": result.get("dataset_id"),
                        "metadata": result.get("metadata"),
                        "records": result.get("records"),
                        "fetched_at": result.get("fetched_at"),
                        "raw_path": result.get("raw_path"),
                    }
                )
                record_count = len(result.get("records") or [])
            except Exception as exc:  # pragma: no cover - defensive
                status = "failed"
                error_message = str(exc)
                payload_text = error_message
                LOGGER.exception("Eurostat extraction failed for %s: %s", selection.dataset_id, exc)

            completed_ts = datetime.utcnow()
            records.append(
                IngestionRecord(
                    selection=selection,
                    payload_format="json",
                    payload=payload_text,
                    records_count=record_count,
                    ingestion_status=status,
                    ingestion_started_at=start_ts,
                    ingestion_completed_at=completed_ts,
                    error_message=error_message,
                )
            )
    return records


async def _run_extractions(buckets: Dict[str, List[Selection]]) -> List[IngestionRecord]:
    """Dispatch extractions per source."""
    all_records: List[IngestionRecord] = []
    for source, items in buckets.items():
        LOGGER.info("Processing source '%s' with %d selection(s)", source, len(items))
        source_lower = source.lower()
        if source_lower == "destatis":
            all_records.extend(await _extract_destatis(items))
        elif source_lower == "gesis":
            all_records.extend(await _extract_gesis(items))
        elif source_lower == "eurostat":
            all_records.extend(await _extract_eurostat(items))
        elif source_lower == "soep":
            all_records.extend(await _extract_soep(items))
        else:
            LOGGER.warning(
                "No extractor registered for source '%s'; marking as failed", source
            )
            timestamp = datetime.utcnow()
            for selection in items:
                all_records.append(
                    IngestionRecord(
                        selection=selection,
                        payload_format="text",
                        payload=f"Extractor not implemented for source '{source}'",
                        records_count=0,
                        ingestion_status="failed",
                        ingestion_started_at=timestamp,
                        ingestion_completed_at=timestamp,
                        error_message="missing_extractor",
                    )
                )
    return all_records


def dispatch_to_sources(
    buckets: Dict[str, List[Selection]], dry_run: bool
) -> List[IngestionRecord]:
    """Execute extraction per source and return prepared ingestion records."""
    if dry_run:
        for source, items in buckets.items():
            LOGGER.info("Dry-run: source '%s' -> %d dataset(s)", source, len(items))
            for item in items:
                LOGGER.debug(
                    "DRY RUN - %s/%s topic=%s score=%.3f decided_at=%s",
                    source,
                    item.dataset_id,
                    item.topic_id,
                    item.score,
                    item.decided_at,
                )
        return []

    return asyncio.run(_run_extractions(buckets))


def persist_records(
    loader: ClickHouseLoader, run_id: uuid.UUID, records: Sequence[IngestionRecord]
) -> None:
    """Persist ingestion records into ClickHouse."""
    if not records:
        return

    rows: List[Tuple[Any, ...]] = []
    for record in records:
        selection = record.selection
        rows.append(
            (
                str(run_id),
                selection.dataset_id,
                selection.source,
                selection.topic_id,
                selection.classifier_version,
                "unknown",  # taxonomy_version placeholder
                selection.metadata_hash or "",
                selection.decided_at,
                record.payload_format,
                record.payload,
                record.records_count,
                record.ingestion_status,
                record.ingestion_started_at,
                record.ingestion_completed_at,
            )
        )

    loader.client.insert(
        "raw.topic_selected_payloads",
        rows,
        column_names=[
            "run_id",
            "dataset_id",
            "source",
            "topic_id",
            "classifier_version",
            "taxonomy_version",
            "metadata_hash",
            "decided_at",
            "payload_format",
            "payload",
            "records_count",
            "ingestion_status",
            "ingestion_started_at",
            "ingestion_completed_at",
        ],
    )


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest topic-selected datasets into ClickHouse."
    )
    parser.add_argument(
        "--sources",
        nargs="*",
        help="Filter to specific metadata sources (e.g. destatis, gesis, soep).",
    )
    parser.add_argument(
        "--topics", nargs="*", help="Filter to specific topic identifiers."
    )
    parser.add_argument("--since", help="ISO timestamp to limit classifier decisions.")
    parser.add_argument(
        "--limit", type=int, help="Maximum number of selections to process."
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=DEFAULT_THRESHOLD,
        help="Minimum classifier score to accept.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print selections without triggering extraction.",
    )
    parser.add_argument(
        "--log-level", default="INFO", help="Logging level (default: INFO)."
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        stream=sys.stdout,
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    run_id = uuid.uuid4()
    LOGGER.info("Starting topic-selected ingestion run %s", run_id)

    try:
        with ClickHouseLoader() as loader:
            ensure_payload_table(loader)
            selections = fetch_selections(
                loader=loader,
                sources=args.sources,
                topics=args.topics,
                since=args.since,
                limit=args.limit,
                min_score=args.min_score,
            )
    except Exception as exc:
        LOGGER.exception("Failed to load selections: %s", exc)
        return 1

    if not selections:
        LOGGER.info("No selections found matching the filters.")
        return 0

    LOGGER.info(
        "Loaded %d selection(s) across %d source(s)",
        len(selections),
        len({sel.source for sel in selections}),
    )

    buckets = bucket_by_source(selections)
    records = dispatch_to_sources(buckets, dry_run=args.dry_run)

    if not args.dry_run and records:
        try:
            with ClickHouseLoader() as loader:
                persist_records(loader, run_id, records)
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.exception("Failed to persist ingestion records: %s", exc)
            return 1

    LOGGER.info("Run %s completed (dry_run=%s)", run_id, args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
