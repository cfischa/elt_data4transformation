"""Status / coverage report tests against real Postgres."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

import pytest

from study_scraper.discovery.base import Candidate
from study_scraper.discovery.openalex import OpenAlexSource
from study_scraper.discovery.ssoar import SSOARSource
from study_scraper.models import CrawlRun
from study_scraper.pipeline import run_one
from study_scraper.status import build_status, format_text
from study_scraper.storage import PostgresStorage
from study_scraper.topics import load_topics


TEST_DSN = os.environ.get("STUDY_SCRAPER_TEST_DSN")
FIXTURES = Path(__file__).resolve().parent / "fixtures"

pytestmark = pytest.mark.skipif(
    not TEST_DSN, reason="STUDY_SCRAPER_TEST_DSN not set; skipping status tests"
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
def topics_list():
    return load_topics(
        Path(__file__).resolve().parents[2] / "config" / "topics" / "topics.csv"
    )


def _klima(topics_list):
    return next(t for t in topics_list if t.id == "klima")


def test_status_empty_db(storage: PostgresStorage) -> None:
    report = build_status(storage)
    assert report.total_studies == 0
    assert report.total_runs == 0
    assert report.keep_rate is None
    assert report.duplicate_rate is None
    assert report.duplicates_total == 0
    assert report.attribution_days_since_last_attempt is None


def _seed_study(storage: PostgresStorage, *, title: str):
    from study_scraper.models import Provenance, Study

    study = Study.build(
        canonical_url=f"https://example.org/{abs(hash(title))}",
        title=title,
        abstract="50% support the policy.",
        publication_date=None,
        fetched_at=datetime(2026, 6, 15, tzinfo=timezone.utc),
        source_id="openalex",
        provenance=Provenance(discovery_source="openalex"),
        topic_ids=["klima"],
        topic_scores={"klima": 0.5},
    )
    storage.upsert_study(study)
    return study


def test_status_attribution_staleness_recent(storage: PostgresStorage) -> None:
    """#110: a fresh attribution_attempts row reads as ~0 days stale."""
    from study_scraper.attribute import apply_responses

    s = _seed_study(storage, title="Recent Attempt")
    response = json.dumps({"attributions": [
        {"question": "Q", "position": "support", "percentage": 50,
         "population": None, "confidence": 0.7},
    ]})
    apply_responses(storage=storage, responses={s.id: response})

    report = build_status(storage)
    assert report.attribution_days_since_last_attempt is not None
    assert 0.0 <= report.attribution_days_since_last_attempt < 1.0

    text = format_text(report)
    assert "attribution last attempt" in text


def test_status_attribution_staleness_stale(storage: PostgresStorage) -> None:
    """A stale (>3 day old) attribution_attempts row must surface as such,
    e.g. the 2026-08-11..2026-08-17 dark-pipeline window from #110."""
    s = _seed_study(storage, title="Stale Attempt")
    stale_ts = datetime.now(timezone.utc) - timedelta(days=6)
    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO study_scraper.attribution_attempts "
                "(study_id, model, found, attempted_at) "
                "VALUES (%s, %s, %s, %s)",
                (s.id, "llm-v1", 0, stale_ts),
            )
        conn.commit()

    report = build_status(storage)
    assert report.attribution_days_since_last_attempt is not None
    assert report.attribution_days_since_last_attempt >= 5.9


def test_status_after_two_source_run(
    storage: PostgresStorage, topics_list
) -> None:
    klima = _klima(topics_list)
    with SSOARSource(from_file=FIXTURES / "ssoar" / "klima_records.xml") as src:
        run_one(source=src, topic=klima, storage=storage)
    with OpenAlexSource(from_file=FIXTURES / "openalex" / "klima_works.json") as src:
        run_one(source=src, topic=klima, storage=storage)

    report = build_status(storage)
    # SSOAR keeps 6, OpenAlex keeps 5 (one is the dedup-test row that
    # collides with an existing SSOAR canonical_url, but the OpenAlex
    # fixture's dedup row uses the openalex.org URL — different
    # canonical_url, so it's a separate study). Total ~11.
    assert report.total_studies >= 10
    assert "klima" in report.studies_per_topic
    assert set(report.studies_per_source) == {"ssoar", "openalex"}
    assert report.total_runs == 2
    assert report.successful_runs == 2
    assert report.failed_runs == 0
    assert report.keep_rate is not None
    assert 0.0 < report.keep_rate <= 1.0


def test_status_reports_duplicate_rate(
    storage: PostgresStorage, topics_list
) -> None:
    """issue #82: re-crawling the same fixture dedups every candidate onto
    the studies the first run already kept; `status` must surface that as
    crawl waste, not silently absorb it into `candidates_kept`."""
    klima = _klima(topics_list)
    with SSOARSource(from_file=FIXTURES / "ssoar" / "klima_records.xml") as src:
        first = run_one(source=src, topic=klima, storage=storage)
    with SSOARSource(from_file=FIXTURES / "ssoar" / "klima_records.xml") as src:
        run_one(source=src, topic=klima, storage=storage)

    report = build_status(storage)
    # The second run's 6 kept candidates all dedup onto the first run's.
    assert report.duplicates_total == first.candidates_kept == 6
    assert report.duplicate_rate is not None
    assert 0.0 < report.duplicate_rate < 1.0


def test_format_text_includes_key_sections(
    storage: PostgresStorage, topics_list
) -> None:
    klima = _klima(topics_list)
    with SSOARSource(from_file=FIXTURES / "ssoar" / "klima_records.xml") as src:
        run_one(source=src, topic=klima, storage=storage)
    text = format_text(build_status(storage))
    assert "study scraper status" in text
    assert "studies per topic" in text
    assert "studies per source" in text
    assert "lake (source_records" in text
    assert "recent runs" in text
    assert "duplicates" in text
    assert "ssoar" in text


class _AbortingSource:
    """Yields nothing and blows up immediately -- simulates a source that
    dies on every request (e.g. an expired API key, issue #48)."""

    source_id = "aborting"

    def iter_candidates(self, topic, *, limit=None):
        raise RuntimeError("simulated 401 Unauthorized")
        yield  # pragma: no cover - unreachable, makes this a generator


def test_status_counts_aborted_run_as_failed(
    storage: PostgresStorage, topics_list
) -> None:
    """#48: a source whose iter_candidates raises before yielding anything
    leaves candidates_seen=0 and errors=0 -- it must still surface as a
    failed run (not a silently "successful" seen=0/errors=0 row)."""
    klima = _klima(topics_list)
    with pytest.raises(RuntimeError, match="simulated 401"):
        run_one(source=_AbortingSource(), topic=klima, storage=storage)

    report = build_status(storage)
    assert report.total_runs == 1
    assert report.failed_runs == 1
    assert report.successful_runs == 0

    text = format_text(report)
    assert "ERR" in text
    assert "simulated 401" in text


def test_status_does_not_flag_in_progress_lake_run_as_failed(
    storage: PostgresStorage,
) -> None:
    """`ingest.py::run_lake_ingest` inserts its `crawl_runs` row early
    (finished_at=None, no `notes`) so `discovery_run_id` has an FK target
    before iteration starts, then fills in `finished_at` once the run
    completes. That in-progress window must read as healthy, not as an
    aborted run -- only a `notes` value starting `aborted:` (set by
    `pipeline.py::run_one` when a source raises mid-run) means failure."""
    run = CrawlRun(
        id=str(uuid.uuid4()),
        source_id="dawum",
        topic_id="__lake__",
        started_at=datetime.now(timezone.utc),
    )
    storage.record_crawl_run(run)

    report = build_status(storage)
    assert report.total_runs == 1
    assert report.failed_runs == 0
    assert report.successful_runs == 1

    text = format_text(report)
    assert "ERR" not in text


def test_status_source_staleness_no_runs(storage: PostgresStorage) -> None:
    """A source that has never run doesn't appear in the staleness map."""
    report = build_status(storage)
    assert report.source_days_since_last_success == {}


def test_status_source_staleness_recent_clean_run(storage: PostgresStorage) -> None:
    """#115: a source's most recent clean run reads as ~0 days stale."""
    run = CrawlRun(
        id=str(uuid.uuid4()),
        source_id="ssoar",
        topic_id="klima",
        started_at=datetime.now(timezone.utc),
        finished_at=datetime.now(timezone.utc),
        errors=0,
    )
    storage.record_crawl_run(run)

    report = build_status(storage)
    days = report.source_days_since_last_success["ssoar"]
    assert days is not None
    assert 0.0 <= days < 1.0

    text = format_text(report)
    assert "crawl staleness per source" in text
    assert "ssoar" in text


def test_status_source_staleness_ignores_errored_runs(
    storage: PostgresStorage,
) -> None:
    """A source whose only runs errored/aborted (e.g. bundestag_dip's
    recurring 401s, #106) must read as never-clean, not as fresh just
    because it kept attempting."""
    run = CrawlRun(
        id=str(uuid.uuid4()),
        source_id="bundestag_dip",
        topic_id="klima",
        started_at=datetime.now(timezone.utc),
        finished_at=None,
        errors=0,
        notes="aborted: 401 Unauthorized",
    )
    storage.record_crawl_run(run)

    report = build_status(storage)
    assert report.source_days_since_last_success["bundestag_dip"] is None


def test_status_source_staleness_uses_last_clean_run_when_stale(
    storage: PostgresStorage,
) -> None:
    """A stale (>3 day old) last-clean-run must surface as such even if
    more recent runs for the same source errored."""
    stale_ts = datetime.now(timezone.utc) - timedelta(days=6)
    clean = CrawlRun(
        id=str(uuid.uuid4()),
        source_id="bundestag_dip",
        topic_id="klima",
        started_at=stale_ts,
        finished_at=stale_ts,
        errors=0,
    )
    storage.record_crawl_run(clean)
    recent_failure = CrawlRun(
        id=str(uuid.uuid4()),
        source_id="bundestag_dip",
        topic_id="klima",
        started_at=datetime.now(timezone.utc),
        finished_at=None,
        errors=0,
        notes="aborted: 401 Unauthorized",
    )
    storage.record_crawl_run(recent_failure)

    report = build_status(storage)
    days = report.source_days_since_last_success["bundestag_dip"]
    assert days is not None
    assert days >= 5.9


def test_status_counts_lake_records(
    storage: PostgresStorage, topics_list
) -> None:
    """A14: source_records / lake counters surface in the status report."""
    from pathlib import Path as _P
    from study_scraper.ingest import run_lake_ingest
    from study_scraper.sources.dawum import DAWUMSource
    from study_scraper.sources.gesis import GESISSource

    with DAWUMSource(
        from_file=_P(__file__).resolve().parent / "fixtures" / "dawum" / "sample.json"
    ) as src:
        run_lake_ingest(source=src, storage=storage)
    with GESISSource(
        from_file=_P(__file__).resolve().parent / "fixtures" / "gesis" / "sample.json"
    ) as src:
        run_lake_ingest(source=src, storage=storage)

    report = build_status(storage)
    assert report.total_source_records >= 5
    assert report.source_records_per_source.get("dawum", 0) >= 1
    assert report.source_records_per_source.get("gesis", 0) >= 1
    assert "gesis_kg_sparql_json" in report.source_records_per_format
    assert "dawum_survey_json" in report.source_records_per_format

    text = format_text(report)
    assert "lake per format" in text
    assert "gesis_kg_sparql_json" in text
