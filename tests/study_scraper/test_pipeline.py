"""End-to-end pipeline tests against real Postgres.

Like test_storage.py these are gated on STUDY_SCRAPER_TEST_DSN; the
suite still runs without it (these are simply skipped).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator

import pytest

from study_scraper.discovery.ssoar import SSOARSource
from study_scraper.pipeline import run_one
from study_scraper.storage import PostgresStorage
from study_scraper.topics import load_topics


TEST_DSN = os.environ.get("STUDY_SCRAPER_TEST_DSN")
FIXTURE = (
    Path(__file__).resolve().parent / "fixtures" / "ssoar" / "klima_records.xml"
)

pytestmark = pytest.mark.skipif(
    not TEST_DSN, reason="STUDY_SCRAPER_TEST_DSN not set; skipping pipeline tests"
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


def test_pipeline_runs_ssoar_fixture_to_postgres(
    storage: PostgresStorage, klima_topic
) -> None:
    with SSOARSource(from_file=FIXTURE) as src:
        run = run_one(source=src, topic=klima_topic, storage=storage)

    # 6 climate-relevant + 1 negative control
    assert run.candidates_seen == 7
    # Negative control "Datenmigration..." has zero klima keywords, so
    # it must be dropped. 6 should be kept.
    assert run.candidates_kept == 6
    assert run.errors == 0

    rows = storage.list_studies(topic_id="klima", limit=100)
    assert len(rows) == 6
    titles = [row["title"] for row in rows]
    assert any("Kommunaler Klimaschutz" in t for t in titles)
    assert any("Energiewende" in t for t in titles)


def test_pipeline_rejects_negative_control(
    storage: PostgresStorage, klima_topic
) -> None:
    with SSOARSource(from_file=FIXTURE) as src:
        run_one(source=src, topic=klima_topic, storage=storage)

    rows = storage.list_studies(limit=100)
    titles = [row["title"] for row in rows]
    assert not any("Datenmigration" in t for t in titles)


def test_pipeline_is_idempotent_across_reruns(
    storage: PostgresStorage, klima_topic
) -> None:
    with SSOARSource(from_file=FIXTURE) as src:
        first = run_one(source=src, topic=klima_topic, storage=storage)
    with SSOARSource(from_file=FIXTURE) as src:
        second = run_one(source=src, topic=klima_topic, storage=storage)

    assert first.candidates_kept == second.candidates_kept
    rows = storage.list_studies(topic_id="klima", limit=100)
    assert len(rows) == 6  # not 12

    # Crawl runs both recorded; both attach to the same studies.
    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS c FROM study_scraper.crawl_runs")
            assert cur.fetchone()["c"] == 2
            cur.execute(
                "SELECT COUNT(*) AS c FROM study_scraper.crawl_run_studies"
            )
            # 6 studies × 2 runs = 12 junction rows
            assert cur.fetchone()["c"] == 12


def test_quantitative_signal_detected(
    storage: PostgresStorage, klima_topic
) -> None:
    with SSOARSource(from_file=FIXTURE) as src:
        run_one(source=src, topic=klima_topic, storage=storage)

    rows = storage.list_studies(topic_id="klima", limit=100)
    # The "Fortschritt der Energiewende im Spiegel..." abstract contains
    # "n=1024" and "Befragung" — both quantitative cues.
    energiewende = next(r for r in rows if "Fortschritt der Energiewende" in r["title"])
    assert energiewende["has_quantitative_data"] is True


def test_topic_scores_populated(
    storage: PostgresStorage, klima_topic
) -> None:
    with SSOARSource(from_file=FIXTURE) as src:
        run_one(source=src, topic=klima_topic, storage=storage)
    rows = storage.list_studies(topic_id="klima", limit=100)
    for row in rows:
        assert "klima" in row["topic_scores"]
        assert 0.0 < row["topic_scores"]["klima"] <= 1.0


def test_last_crawl_finished_at_tracks_completed_runs(
    storage: PostgresStorage, klima_topic
) -> None:
    """Feeds the SSOAR OAI `from=` incremental window (issue #34): None
    before any run, then the run's finished_at once one completes."""
    assert storage.last_crawl_finished_at(
        source_id="ssoar", topic_id="klima"
    ) is None

    with SSOARSource(from_file=FIXTURE) as src:
        run = run_one(source=src, topic=klima_topic, storage=storage)

    latest = storage.last_crawl_finished_at(source_id="ssoar", topic_id="klima")
    assert latest is not None
    assert latest == run.finished_at

    # A different (source, topic) pair stays untouched.
    assert storage.last_crawl_finished_at(
        source_id="openalex", topic_id="klima"
    ) is None


def test_openalex_citation_graph_propagated_to_provenance(
    storage: PostgresStorage, klima_topic
) -> None:
    """Phase 5d step 1: OpenAlex referenced_works / related_works land
    in studies.provenance so the future reference-follower has data."""
    from study_scraper.discovery.openalex import OpenAlexSource

    oa_fixture = (
        FIXTURE.parent.parent / "openalex" / "klima_works.json"
    )
    with OpenAlexSource(from_file=oa_fixture) as src:
        run_one(source=src, topic=klima_topic, storage=storage)

    # Use a JSONB query to find any study with referenced_works set.
    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT title, "
                "       jsonb_array_length(provenance->'referenced_works') AS n_ref, "
                "       jsonb_array_length(provenance->'related_works')    AS n_rel "
                "FROM   study_scraper.studies "
                "WHERE  provenance ? 'referenced_works' "
                "ORDER  BY n_ref DESC NULLS LAST "
                "LIMIT  5"
            )
            rows = cur.fetchall()
    assert rows, "no studies carried referenced_works through to provenance"
    # The Forsa fixture record has 2 referenced + 3 related.
    forsa = next(r for r in rows if "Forsa-Umfrage" in r["title"])
    assert forsa["n_ref"] == 2
    assert forsa["n_rel"] == 3
