"""Status / coverage report tests against real Postgres."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator

import pytest

from study_scraper.discovery.openalex import OpenAlexSource
from study_scraper.discovery.ssoar import SSOARSource
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
    assert "ssoar" in text


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
