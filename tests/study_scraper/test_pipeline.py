"""End-to-end pipeline tests against real Postgres.

Like test_storage.py these are gated on STUDY_SCRAPER_TEST_DSN; the
suite still runs without it (these are simply skipped).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator

import pytest

from study_scraper.discovery.base import Candidate
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


class _AbortingSource:
    """A source that yields one candidate then blows up mid-pagination,
    simulating an OAI resumption-token walk exhausting retries on page 2."""

    source_id = "aborting"

    def iter_candidates(self, topic, *, limit=None):
        yield Candidate(
            source_id=self.source_id,
            external_id="1",
            canonical_url="https://example.org/aborting-1",
            title="Kommunaler Klimaschutz erste Seite",
        )
        raise RuntimeError("simulated pagination failure")


def test_aborted_run_does_not_advance_last_crawl_finished_at(
    storage: PostgresStorage, klima_topic
) -> None:
    """issue #34 follow-up: a run whose source raises mid-pagination must
    not look like a clean completion to `last_crawl_finished_at()` --
    otherwise the next incremental `from=` window silently skips whatever
    the aborted run never reached."""
    assert storage.last_crawl_finished_at(
        source_id="aborting", topic_id="klima"
    ) is None

    with pytest.raises(RuntimeError, match="simulated pagination failure"):
        run_one(source=_AbortingSource(), topic=klima_topic, storage=storage)

    # The failed run must not count as "completed".
    assert storage.last_crawl_finished_at(
        source_id="aborting", topic_id="klima"
    ) is None

    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT finished_at, notes, candidates_seen, errors "
                "FROM study_scraper.crawl_runs WHERE source_id = 'aborting'"
            )
            row = cur.fetchone()
    assert row is not None
    assert row["finished_at"] is None
    assert row["candidates_seen"] == 1  # the one candidate seen before the raise
    assert row["notes"] is not None and "aborted" in row["notes"]
    # issue #48: an aborted run (e.g. a 401 mid-pagination) must count in
    # `errors` too -- `study status` classifies runs as failed by
    # `errors > 0` alone, so an uncounted abort read as a clean
    # `seen=0 kept=0 errors=0` row and stayed invisible for weeks.
    assert row["errors"] == 1


class _DoiDedupSource:
    """Two candidates sharing a DOI under different external ids --
    simulates the same study being (re)discovered from a second source
    or URL, which triggers upsert_study's DOI-dedup short-circuit and
    rewrites `study.id` in place (issue #79)."""

    source_id = "doi_dedup_test"

    def iter_candidates(self, topic, *, limit=None):
        yield Candidate(
            source_id=self.source_id,
            external_id="a",
            canonical_url="https://example.org/doi-dedup-a",
            title="Kommunaler Klimaschutz und Energiewende Umfrage",
            abstract="62 Prozent der Befragten befürworten den Klimaschutz.",
            doi="10.9999/dedup-fixture-79",
        )
        yield Candidate(
            source_id=self.source_id,
            external_id="b",
            canonical_url="https://example.org/doi-dedup-b",
            title="Kommunaler Klimaschutz und Energiewende Umfrage (Reprint)",
            abstract="55 Prozent der Befragten lehnen weitere Maßnahmen ab.",
            doi="10.9999/dedup-fixture-79",
        )


def test_claims_survive_doi_dedup(
    storage: PostgresStorage, klima_topic
) -> None:
    """issue #79: a candidate that DOI-dedups onto an existing study must
    not raise claims_study_id_fkey -- its claims must land under the
    canonical (post-dedup) study id, not the pre-dedup one that was never
    inserted."""
    run = run_one(source=_DoiDedupSource(), topic=klima_topic, storage=storage)
    assert run.errors == 0

    rows = storage.list_studies(limit=100)
    matching = [r for r in rows if r["doi"] == "10.9999/dedup-fixture-79"]
    assert len(matching) == 1  # second candidate deduped onto the first
    canonical_id = matching[0]["id"]

    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT study_id FROM study_scraper.claims WHERE study_id = %s",
                (canonical_id,),
            )
            claim_rows = cur.fetchall()
    # `upsert_claims` replaces the prior extractor pass for a study, so
    # only the second (deduped) candidate's claim remains -- but it must
    # be attributed to the canonical id, not silently dropped/rejected.
    assert len(claim_rows) >= 1
    assert all(r["study_id"] == canonical_id for r in claim_rows)


def test_duplicate_candidates_counted_in_run_parameters(
    storage: PostgresStorage, klima_topic
) -> None:
    """issue #82: a candidate that dedups onto an existing study (DOI or
    title match) must be counted so `status`/`status --json` can report
    crawl waste, not silently disappear from the run's bookkeeping."""
    run = run_one(source=_DoiDedupSource(), topic=klima_topic, storage=storage)

    # 2 candidates seen, 1 new study kept, 1 deduped onto it.
    assert run.candidates_seen == 2
    assert run.candidates_kept == 2  # both land in kept_ids (one is a dup)
    assert run.parameters["duplicates"] == 1


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
