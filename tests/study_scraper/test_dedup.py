"""Tests for Phase 5b cross-source dedup (DOI-based for now;
title-near-duplicate via pg_trgm lands in the next iteration)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import pytest

from study_scraper.discovery.openalex import OpenAlexSource
from study_scraper.discovery.ssoar import SSOARSource
from study_scraper.models import Provenance, Study
from study_scraper.pipeline import _normalize_doi, run_one
from study_scraper.storage import PostgresStorage
from study_scraper.topics import load_topics


TEST_DSN = os.environ.get("STUDY_SCRAPER_TEST_DSN")
FIXTURES = Path(__file__).resolve().parent / "fixtures"

pytestmark = pytest.mark.skipif(
    not TEST_DSN, reason="STUDY_SCRAPER_TEST_DSN not set; skipping dedup tests"
)


@pytest.fixture(scope="module")
def storage() -> PostgresStorage:
    assert TEST_DSN is not None
    store = PostgresStorage(TEST_DSN)
    store.migrate()
    return store


@pytest.fixture(autouse=True)
def _clean(storage: PostgresStorage) -> Iterator[None]:
    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE study_scraper.claims CASCADE")
            cur.execute("TRUNCATE study_scraper.crawl_run_studies CASCADE")
            cur.execute("TRUNCATE study_scraper.studies CASCADE")
            cur.execute("TRUNCATE study_scraper.crawl_runs CASCADE")
        conn.commit()
    yield


@pytest.fixture(scope="module")
def klima_topic():
    return next(
        t for t in load_topics(
            Path(__file__).resolve().parents[2] / "config" / "topics" / "topics.csv"
        ) if t.id == "klima"
    )


def _provenance(source: str) -> Provenance:
    return Provenance(discovery_source=source, discovery_query="klima")


def _study(
    *, canonical_url: str, doi: str | None, source_id: str, title: str = "A study",
) -> Study:
    return Study.build(
        canonical_url=canonical_url,
        source_urls=[canonical_url],
        title=title,
        fetched_at=datetime(2026, 5, 28, tzinfo=timezone.utc),
        source_id=source_id,
        provenance=_provenance(source_id),
        doi=doi,
        topic_ids=["klima"],
        topic_scores={"klima": 0.5},
    )


# --------------------------------------------------------------------------
# DOI normalization (pure unit)
# --------------------------------------------------------------------------


class TestNormalizeDoi:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("https://doi.org/10.1515/pwp-2023-0031", "10.1515/pwp-2023-0031"),
            ("http://doi.org/10.1515/pwp-2023-0031",  "10.1515/pwp-2023-0031"),
            ("doi:10.1515/pwp-2023-0031",              "10.1515/pwp-2023-0031"),
            ("DOI:10.1515/pwp-2023-0031",              "10.1515/pwp-2023-0031"),
            ("10.1515/pwp-2023-0031",                  "10.1515/pwp-2023-0031"),
            ("",                                       None),
            (None,                                     None),
            ("   ",                                    None),
        ],
    )
    def test_normalize(self, raw, expected):
        assert _normalize_doi(raw) == expected


# --------------------------------------------------------------------------
# DOI dedup at upsert
# --------------------------------------------------------------------------


def test_first_doi_insert_is_a_new_row(storage: PostgresStorage) -> None:
    s = _study(
        canonical_url="https://doi.org/10.1515/pwp-2023-0031",
        doi="10.1515/pwp-2023-0031",
        source_id="openalex",
    )
    assert storage.upsert_study(s) is True
    row = storage.get_study(s.id)
    assert row is not None
    assert row["doi"] == "10.1515/pwp-2023-0031"


def test_second_source_same_doi_merges_into_existing(
    storage: PostgresStorage,
) -> None:
    # First ingest under one URL
    first = _study(
        canonical_url="https://openalex.org/W4391234567",
        doi="10.1515/pwp-2023-0031",
        source_id="openalex",
        title="CO₂-Bepreisung in Deutschland",
    )
    storage.upsert_study(first)

    # A second source surfaces the same study under a different URL
    second = _study(
        canonical_url="https://www.ssoar.info/ssoar/handle/document/99999",
        doi="10.1515/pwp-2023-0031",
        source_id="ssoar",
        title="Different title, but same DOI",
    )
    is_new = storage.upsert_study(second)
    # The new candidate is NOT a new row — it merged into the existing.
    assert is_new is False

    # Still exactly one row by DOI
    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS c FROM study_scraper.studies "
                "WHERE doi = '10.1515/pwp-2023-0031'"
            )
            assert cur.fetchone()["c"] == 1

    # The existing row picked up the new URL into source_urls; primary
    # fields stayed frozen (title not overwritten).
    row = storage.get_study(first.id)
    assert row is not None
    assert row["title"] == "CO₂-Bepreisung in Deutschland"
    assert "https://www.ssoar.info/ssoar/handle/document/99999" in row["source_urls"]
    assert "https://openalex.org/W4391234567" in row["source_urls"]


def test_dedup_mutates_study_id_to_target(storage: PostgresStorage) -> None:
    """After dedup the in-memory Study has the target id so the caller
    (pipeline) attaches claims and crawl_run_studies to the right row."""
    first = _study(
        canonical_url="https://example.org/a",
        doi="10.9999/aaa",
        source_id="openalex",
    )
    storage.upsert_study(first)

    second = _study(
        canonical_url="https://example.org/b",
        doi="10.9999/aaa",
        source_id="ssoar",
    )
    storage.upsert_study(second)
    assert second.id == first.id  # mutated after dedup


def test_dedup_merges_topic_ids_and_scores(storage: PostgresStorage) -> None:
    first = _study(
        canonical_url="https://example.org/a",
        doi="10.9999/bbb",
        source_id="openalex",
    )
    first.topic_ids = ["klima"]
    first.topic_scores = {"klima": 0.5}
    storage.upsert_study(first)

    # A different topic catches the same study via another source
    second = _study(
        canonical_url="https://example.org/b",
        doi="10.9999/bbb",
        source_id="ssoar",
    )
    second.topic_ids = ["migration_einwanderung"]
    second.topic_scores = {"migration_einwanderung": 0.4}
    storage.upsert_study(second)

    row = storage.get_study(first.id)
    assert set(row["topic_ids"]) == {"klima", "migration_einwanderung"}
    assert row["topic_scores"]["klima"] == 0.5
    assert row["topic_scores"]["migration_einwanderung"] == 0.4


def test_no_doi_no_title_match_no_dedup(storage: PostgresStorage) -> None:
    """Without a DOI AND with different titles, no dedup -- both rows kept."""
    a = _study(
        canonical_url="https://a.example/x", doi=None, source_id="ssoar",
        title="A study about taxes",
    )
    b = _study(
        canonical_url="https://b.example/x", doi=None, source_id="openalex",
        title="A wholly different study about migration",
    )
    storage.upsert_study(a)
    storage.upsert_study(b)
    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS c FROM study_scraper.studies")
            assert cur.fetchone()["c"] == 2


# --------------------------------------------------------------------------
# Title-near-duplicate dedup (Phase 5b follow-on, migration 0006)
# --------------------------------------------------------------------------


def test_identical_title_no_doi_merges(storage: PostgresStorage) -> None:
    """Without DOIs, identical titles + same publication_year merge."""
    a = _study(
        canonical_url="https://a.example/x", doi=None, source_id="ssoar",
        title="Akzeptanz der Energiewende in Deutschland",
    )
    b = _study(
        canonical_url="https://b.example/x", doi=None, source_id="openalex",
        title="Akzeptanz der Energiewende in Deutschland",
    )
    storage.upsert_study(a)
    is_new = storage.upsert_study(b)
    assert is_new is False  # merged
    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS c FROM study_scraper.studies")
            assert cur.fetchone()["c"] == 1


def test_near_identical_title_merges_above_threshold(
    storage: PostgresStorage,
) -> None:
    """Near-identical titles (e.g. plural / case difference) merge."""
    a = _study(
        canonical_url="https://a.example/x", doi=None, source_id="ssoar",
        title="Erdgas für den Klimaschutz? Chancen und Risiken einer verstärkten Gasnutzung in Europa",
    )
    b = _study(
        canonical_url="https://b.example/y", doi=None, source_id="openalex",
        # Very small wording variant; should still match at sim >= 0.85.
        title="Erdgas für den Klimaschutz - Chancen und Risiken einer verstärkten Gasnutzung in Europa",
    )
    storage.upsert_study(a)
    is_new = storage.upsert_study(b)
    assert is_new is False
    row = storage.get_study(a.id)
    # Both URLs now in source_urls.
    assert "https://a.example/x" in row["source_urls"]
    assert "https://b.example/y" in row["source_urls"]


def test_different_titles_do_not_merge(storage: PostgresStorage) -> None:
    """Two genuinely different titles do NOT merge even at same year."""
    a = _study(
        canonical_url="https://a.example/x", doi=None, source_id="ssoar",
        title="Kommunaler Klimaschutz in Deutschland",
    )
    b = _study(
        canonical_url="https://b.example/y", doi=None, source_id="openalex",
        title="Akzeptanz der Energiewende in Deutschland",
    )
    storage.upsert_study(a)
    is_new = storage.upsert_study(b)
    assert is_new is True
    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS c FROM study_scraper.studies")
            assert cur.fetchone()["c"] == 2


def test_doi_dedup_runs_before_title_dedup(storage: PostgresStorage) -> None:
    """DOI match short-circuits before title lookup -- DOI is the
    authoritative identifier when present."""
    a = _study(
        canonical_url="https://a.example/x",
        doi="10.1111/abc",
        source_id="ssoar",
        title="Study title v1",
    )
    b = _study(
        canonical_url="https://b.example/y",
        doi="10.1111/abc",
        source_id="openalex",
        title="Totally different title that wouldn't trigger trigram",
    )
    storage.upsert_study(a)
    is_new = storage.upsert_study(b)
    assert is_new is False
    row = storage.get_study(a.id)
    # DOI dedup wins -- title stays the FIRST one (a's).
    assert row["title"] == "Study title v1"
    assert "https://b.example/y" in row["source_urls"]


def test_title_dedup_promotes_doi_into_existing_row(
    storage: PostgresStorage,
) -> None:
    """When the existing row has no DOI but the new candidate brings
    one, the DOI gets promoted into the existing row via COALESCE."""
    a = _study(
        canonical_url="https://a.example/x", doi=None, source_id="ssoar",
        title="Akzeptanz der Energiewende in Deutschland",
    )
    storage.upsert_study(a)
    b = _study(
        canonical_url="https://b.example/y",
        doi="10.1007/s11578-021-0099-2",
        source_id="openalex",
        title="Akzeptanz der Energiewende in Deutschland",
    )
    storage.upsert_study(b)
    row = storage.get_study(a.id)
    assert row["doi"] == "10.1007/s11578-021-0099-2"


def test_title_dedup_skipped_when_different_publication_year(
    storage: PostgresStorage,
) -> None:
    """Same title published in different years stays separate."""
    from datetime import date
    a = _study(
        canonical_url="https://a.example/x", doi=None, source_id="ssoar",
        title="Politbarometer-Kumulation",
    )
    a.publication_date = date(2019, 1, 1)
    b = _study(
        canonical_url="https://b.example/y", doi=None, source_id="openalex",
        title="Politbarometer-Kumulation",
    )
    b.publication_date = date(2024, 1, 1)
    storage.upsert_study(a)
    is_new = storage.upsert_study(b)
    assert is_new is True  # different years -> separate rows
    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS c FROM study_scraper.studies")
            assert cur.fetchone()["c"] == 2


# --------------------------------------------------------------------------
# End-to-end: the real Erdgas duplicate collapses with the new fixtures
# --------------------------------------------------------------------------


def test_real_fixtures_dedup_via_openalex_doi(
    storage: PostgresStorage, klima_topic
) -> None:
    """Re-run both klima fixtures end-to-end. With DOIs populated in the
    OpenAlex fixture, the CO₂-Bepreisung record stays a single row even
    if it shows up under different URL patterns."""
    with SSOARSource(from_file=FIXTURES / "ssoar" / "klima_records.xml") as src:
        run_one(source=src, topic=klima_topic, storage=storage)
    with OpenAlexSource(from_file=FIXTURES / "openalex" / "klima_works.json") as src:
        run_one(source=src, topic=klima_topic, storage=storage)

    # CO₂-Bepreisung is in OpenAlex only (no SSOAR analog in our fixtures);
    # exactly one row.
    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS c FROM study_scraper.studies "
                "WHERE doi = '10.1515/pwp-2023-0031'"
            )
            assert cur.fetchone()["c"] == 1
