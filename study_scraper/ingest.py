"""Lake-style ingestion orchestrator (Q16-v2).

Parallel to `pipeline.py` (which is topic-driven and writes `studies`).
This module ingests structured-data sources whose payloads land
**as-is** in `source_records`. No topic filter at ingest time -- topic
association happens later, via views over the lake.

API:
    run_lake_ingest(source, storage, limit, topic_ids) -> CrawlRun

For each `SourceRecord` the source yields:
  - sha256 content-hash matches an existing row -> skip (no change),
  - otherwise upsert. Existing 'rejected'/'kept' status sticks
    (Q12 still applies to source_records too).
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Iterable, Iterator, List, Optional, Protocol

from study_scraper.models import CrawlRun, SourceRecord
from study_scraper.storage import PostgresStorage


LOGGER = logging.getLogger(__name__)


class LakeSource(Protocol):
    """The contract for a `study_scraper/sources/` source."""

    source_id: str

    def iter_records(
        self,
        *,
        run_id: str,
        limit: Optional[int] = None,
    ) -> Iterator[SourceRecord]:
        ...


def run_lake_ingest(
    *,
    source: LakeSource,
    storage: PostgresStorage,
    limit: Optional[int] = None,
    topic_ids: Optional[List[str]] = None,
    notes: Optional[str] = None,
) -> CrawlRun:
    """Run one lake source to completion. Returns the recorded CrawlRun."""
    run = CrawlRun(
        id=str(uuid.uuid4()),
        source_id=source.source_id,
        topic_id="__lake__",  # marker: this run wrote to source_records,
                              # not studies. Schema requires topic_id NOT NULL.
        started_at=datetime.now(timezone.utc),
        parameters={"limit": limit, "topic_ids": topic_ids or []},
        notes=notes,
    )
    # Persist the run early so `discovery_run_id` FK on source_records
    # has something to point at.
    storage.record_crawl_run(run)

    seen = 0
    kept_new = 0
    errors = 0
    try:
        for record in source.iter_records(run_id=run.id, limit=limit):
            seen += 1
            try:
                if topic_ids:
                    # Operator-applied topic tagging at ingest time.
                    # Pure metadata; the lake's payload is untouched.
                    record.topic_ids = list(
                        {*(record.topic_ids or []), *topic_ids}
                    )
                is_new = storage.upsert_source_record(record, status="kept")
                if is_new:
                    kept_new += 1
                LOGGER.info(
                    "%s record %s%s",
                    source.source_id,
                    record.source_record_id or record.canonical_url,
                    " (new)" if is_new else " (existing)",
                )
            except Exception as exc:  # pragma: no cover - logged + counted
                errors += 1
                LOGGER.exception(
                    "%s ingest error on %s: %s",
                    source.source_id,
                    getattr(record, "canonical_url", "<unknown>"),
                    exc,
                )
    finally:
        run = run.model_copy(
            update={
                "finished_at": datetime.now(timezone.utc),
                "candidates_seen": seen,
                "candidates_kept": kept_new,
                "errors": errors,
            }
        )
        storage.record_crawl_run(run)

    LOGGER.info(
        "lake run %s done: source=%s seen=%d new=%d errors=%d",
        run.id, source.source_id, seen, kept_new, errors,
    )
    return run
