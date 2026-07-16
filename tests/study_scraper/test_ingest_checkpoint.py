"""Unit tests for `run_lake_ingest` partial-progress checkpointing.

Regression coverage for #56: a lake ingest run killed mid-loop (e.g. by an
external `timeout` wrapper) must not leave a false-clean `seen=0 kept=0`
`crawl_runs` row -- the loop's `finally` block never runs on a hard kill,
so real progress has to be checkpointed periodically instead. These tests
use fakes (no Postgres) and simulate a "kill" as an unhandled exception
raised from the source's iterator partway through.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterator, List, Optional

import pytest

from study_scraper.ingest import _CHECKPOINT_EVERY, run_lake_ingest
from study_scraper.models import CrawlRun, SourceRecord


class _FakeStorage:
    """Records every `record_crawl_run` call so tests can inspect the
    sequence of checkpoints, and upserts source records in memory."""

    def __init__(self) -> None:
        self.recorded_runs: List[CrawlRun] = []
        self.upserted: List[SourceRecord] = []

    def record_crawl_run(self, run: CrawlRun) -> None:
        self.recorded_runs.append(run)

    def upsert_source_record(self, record: SourceRecord, *, status: str) -> bool:
        self.upserted.append(record)
        return True  # every record is "new" for these tests


class _KillableSource:
    """Yields `total` records, then raises partway through if `kill_at`
    is set -- simulating a process kill mid-loop."""

    source_id = "fake_lake"

    def __init__(self, total: int, kill_at: Optional[int] = None) -> None:
        self.total = total
        self.kill_at = kill_at

    def iter_records(
        self, *, run_id: str, limit: Optional[int] = None
    ) -> Iterator[SourceRecord]:
        for i in range(self.total):
            if self.kill_at is not None and i == self.kill_at:
                raise RuntimeError("simulated kill")
            yield SourceRecord.build(
                source_id=self.source_id,
                canonical_url=f"https://fake.example/{i}",
                format="dummy",
                content_hash=f"hash-{i}",
                fetched_at=datetime.now(timezone.utc),
                discovery_run_id=run_id,
                payload={"i": i},
            )


def test_checkpoint_written_periodically() -> None:
    storage = _FakeStorage()
    source = _KillableSource(total=_CHECKPOINT_EVERY * 2 + 3)
    run = run_lake_ingest(source=source, storage=storage)

    assert run.candidates_seen == _CHECKPOINT_EVERY * 2 + 3
    assert run.candidates_kept == _CHECKPOINT_EVERY * 2 + 3
    # Initial insert + 2 mid-loop checkpoints + final `finally` write.
    seen_values = [r.candidates_seen for r in storage.recorded_runs]
    assert 0 in seen_values  # the initial pre-loop row
    assert _CHECKPOINT_EVERY in seen_values
    assert _CHECKPOINT_EVERY * 2 in seen_values
    assert seen_values[-1] == _CHECKPOINT_EVERY * 2 + 3


def test_checkpoint_recorded_before_a_later_fatal_error() -> None:
    """A hard process kill (e.g. the `timeout` wrapper's SIGKILL) never
    reaches Python's `finally`, so the *only* durable record of progress
    is whatever was checkpointed mid-loop. This test can't literally kill
    the process, but it proves the checkpoint written at record 25 already
    holds accurate non-zero counts well before the run concludes --
    exactly the state a SIGKILL would freeze `crawl_runs` at.
    """
    storage = _FakeStorage()
    kill_at = _CHECKPOINT_EVERY + 5
    source = _KillableSource(total=_CHECKPOINT_EVERY * 3, kill_at=kill_at)

    with pytest.raises(RuntimeError, match="simulated kill"):
        run_lake_ingest(source=source, storage=storage)

    # The mid-loop checkpoint at record 25 is already durable in storage,
    # independent of the `finally` block that runs afterwards here.
    checkpoint = next(
        r for r in storage.recorded_runs if r.candidates_seen == _CHECKPOINT_EVERY
    )
    assert checkpoint.candidates_seen > 0
    assert checkpoint.finished_at is None
