"""Tests for the reference follower (Phase 5d step 2).

The ID-diffing / end-to-end tests need a real Postgres reachable via
`STUDY_SCRAPER_TEST_DSN` (same gating as test_storage.py / test_pipeline.py)
and are skipped when it's absent. The batching test stubs out the OpenAlex
source and pipeline entirely, so no network or live source hits the API in
any of these — fixture/stub-based throughout.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, List

import pytest

from study_scraper.discovery.openalex import OpenAlexSource
from study_scraper.follow import FETCH_BATCH, fetch_references, pending_references
from study_scraper.models import CrawlRun, Provenance, Study
from study_scraper.pipeline import run_one
from study_scraper.storage import PostgresStorage
from study_scraper.topics import load_topics


TEST_DSN = os.environ.get("STUDY_SCRAPER_TEST_DSN")
FIXTURE = (
    Path(__file__).resolve().parent / "fixtures" / "openalex" / "klima_works.json"
)

pytestmark = pytest.mark.skipif(
    not TEST_DSN, reason="STUDY_SCRAPER_TEST_DSN not set; skipping follow tests"
)


@pytest.fixture(scope="module")
def storage() -> PostgresStorage:
    assert TEST_DSN is not None
    store = PostgresStorage(TEST_DSN)
    store.migrate()
    return store


@pytest.fixture(autouse=True)
def _clean_tables(storage: PostgresStorage) -> Iterator[None]:
    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE study_scraper.crawl_run_studies CASCADE")
            cur.execute("TRUNCATE study_scraper.studies CASCADE")
            cur.execute("TRUNCATE study_scraper.crawl_runs CASCADE")
        conn.commit()
    yield


@pytest.fixture(scope="module")
def klima_topic():
    topics = load_topics(
        Path(__file__).resolve().parents[2] / "config" / "topics" / "topics.csv"
    )
    return next(t for t in topics if t.id == "klima")


def _seed_study(canonical_url: str, **provenance_extra: object) -> Study:
    return Study.build(
        canonical_url=canonical_url,
        title="Seed study",
        fetched_at=datetime(2026, 5, 5, 12, 0, tzinfo=timezone.utc),
        source_id="openalex",
        provenance=Provenance(discovery_source="openalex", **provenance_extra),
        topic_ids=["klima"],
    )


class TestPendingReferences:
    def test_diffs_known_vs_unknown_ids(self, storage: PostgresStorage) -> None:
        # `cited` references three OpenAlex works; W2 is already ingested
        # as its own study row, so only W1/W3 should come back pending.
        cited = _seed_study(
            "https://doi.org/10.1/seed",
            referenced_works=[
                "https://openalex.org/W1",
                "https://openalex.org/W2",
                "https://openalex.org/W3",
            ],
        )
        storage.upsert_study(cited, status="kept")
        already = _seed_study("https://openalex.org/W2")
        storage.upsert_study(already, status="kept")

        pending = pending_references(storage)

        assert set(pending) == {
            "https://openalex.org/W1",
            "https://openalex.org/W3",
        }

    def test_respects_limit(self, storage: PostgresStorage) -> None:
        cited = _seed_study(
            "https://doi.org/10.1/seed2",
            referenced_works=[f"https://openalex.org/W{i}" for i in range(10)],
        )
        storage.upsert_study(cited, status="kept")

        assert len(pending_references(storage, limit=3)) == 3


class TestFetchReferencesBatching:
    def test_batches_pending_ids_in_groups_of_fetch_batch(
        self, storage: PostgresStorage, klima_topic, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        total = FETCH_BATCH + 5
        cited = _seed_study(
            "https://doi.org/10.1/seed3",
            referenced_works=[
                f"https://openalex.org/W{i}" for i in range(total)
            ],
        )
        storage.upsert_study(cited, status="kept")

        seen_batches: List[List[str]] = []

        class _StubSource:
            def __init__(self, *, work_ids: List[str], **_ignored: object) -> None:
                seen_batches.append(list(work_ids))

            def __enter__(self) -> "_StubSource":
                return self

            def __exit__(self, *exc: object) -> bool:
                return False

        def _stub_run_one(
            *, source: object, topic: object, storage: object, min_score: float = 0.2
        ) -> CrawlRun:
            return CrawlRun(
                id="stub",
                source_id="openalex",
                topic_id=klima_topic.id,
                started_at=datetime.now(timezone.utc),
                candidates_seen=1,
                candidates_kept=1,
            )

        monkeypatch.setattr(
            "study_scraper.discovery.openalex.OpenAlexSource", _StubSource
        )
        monkeypatch.setattr("study_scraper.pipeline.run_one", _stub_run_one)

        runs = fetch_references(storage=storage, topic=klima_topic, limit=total)

        assert [len(batch) for batch in seen_batches] == [FETCH_BATCH, 5]
        assert len(runs) == 2
        assert sum(run.candidates_kept for run in runs) == 2


class TestFetchedCandidatePipeline:
    def test_goes_through_topic_filter_and_tags_discovery_method(
        self, storage: PostgresStorage, klima_topic
    ) -> None:
        # `from_file` supplies the OpenAlex response so no network call is
        # made; `work_ids` being set is what real reference-follower fetches
        # use to select this mode, and drives the discovery_method tag.
        with OpenAlexSource(from_file=FIXTURE, work_ids=["W_unused"]) as src:
            run = run_one(source=src, topic=klima_topic, storage=storage)

        assert run.candidates_kept > 0
        rows = storage.list_studies(topic_id="klima", limit=100)
        assert rows
        assert all(
            row["provenance"].get("discovery_method") == "reference_follower"
            for row in rows
        )

    def test_idempotent_on_rerun(
        self, storage: PostgresStorage, klima_topic
    ) -> None:
        with OpenAlexSource(from_file=FIXTURE, work_ids=["W_unused"]) as src:
            run_one(source=src, topic=klima_topic, storage=storage)
        first_count = len(storage.list_studies(topic_id="klima", limit=100))

        with OpenAlexSource(from_file=FIXTURE, work_ids=["W_unused"]) as src:
            run_one(source=src, topic=klima_topic, storage=storage)
        second_count = len(storage.list_studies(topic_id="klima", limit=100))

        assert second_count == first_count
