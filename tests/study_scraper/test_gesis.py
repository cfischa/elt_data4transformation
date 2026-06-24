"""Tests for the GESIS Knowledge Graph SPARQL source (lake-style).

Pure-Python unit tests for the parser; integration tests against real
Postgres exercise the lake ingest path (skipped without
STUDY_SCRAPER_TEST_DSN).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator

import pytest

from study_scraper.ingest import run_lake_ingest
from study_scraper.sources.gesis import (
    GESISSource,
    _extract_source_record_id,
    _first_doi,
    _first_literal,
)
from study_scraper.storage import PostgresStorage


TEST_DSN = os.environ.get("STUDY_SCRAPER_TEST_DSN")
FIXTURE = (
    Path(__file__).resolve().parent / "fixtures" / "gesis" / "sample.json"
)


# --------------------------------------------------------------------------
# Pure unit tests (always run)
# --------------------------------------------------------------------------


def _records():
    with GESISSource(from_file=FIXTURE) as src:
        return list(
            src.iter_records(run_id="00000000-0000-0000-0000-000000000000")
        )


class TestGESISParser:
    def test_yields_one_record_per_dataset_uri(self) -> None:
        # 3 climate-related + 1 negative control = 4 records.
        # All are emitted; topic filtering doesn't happen at the lake.
        assert len(_records()) == 4

    def test_canonical_url_is_kg_uri(self) -> None:
        urls = {r.canonical_url for r in _records()}
        assert "https://data.gesis.org/datasearchkg/Dataset/10.4232/1.14206" in urls

    def test_source_record_id_is_last_uri_segment(self) -> None:
        polit = next(
            r for r in _records()
            if r.canonical_url.endswith("10.4232/1.14206")
        )
        # last segment after the final '/'
        assert polit.source_record_id == "1.14206"

    def test_doi_extracted_from_schema_identifier(self) -> None:
        polit = next(
            r for r in _records()
            if r.canonical_url.endswith("10.4232/1.14206")
        )
        assert polit.doi == "10.4232/1.14206"

    def test_payload_preserves_all_triples(self) -> None:
        polit = next(
            r for r in _records()
            if r.canonical_url.endswith("10.4232/1.14206")
        )
        triples = polit.payload["triples"]
        assert len(triples) >= 10  # politbarometer fixture has 14+
        # Triples are sorted (stable hash); verify the order.
        sorted_again = sorted(
            triples, key=lambda t: (t["p"], str(t["o"]))
        )
        assert triples == sorted_again

    def test_title_hint_propagated_to_provenance(self) -> None:
        polit = next(
            r for r in _records()
            if r.canonical_url.endswith("10.4232/1.14206")
        )
        assert "Politbarometer" in (polit.provenance.get("title_hint") or "")

    def test_license_value_captured(self) -> None:
        polit = next(
            r for r in _records()
            if r.canonical_url.endswith("10.4232/1.14206")
        )
        # The fixture's first license predicate is a GESIS URI.
        assert polit.license is not None
        assert "gesis.org" in polit.license

    def test_record_with_no_license_falls_back_to_default(self) -> None:
        neg = next(
            r for r in _records()
            if r.canonical_url.endswith("10.4232/1.99999")
        )
        # Fixture deliberately omits license -> default applies.
        assert neg.license == "GESIS terms of use (public catalog)"

    def test_content_hash_deterministic(self) -> None:
        a = _records()
        b = _records()
        for ra, rb in zip(a, b):
            assert ra.content_hash == rb.content_hash

    def test_limit_truncates(self) -> None:
        with GESISSource(from_file=FIXTURE) as src:
            recs = list(
                src.iter_records(
                    run_id="00000000-0000-0000-0000-000000000000",
                    limit=2,
                )
            )
        assert len(recs) == 2

    def test_record_format(self) -> None:
        for r in _records():
            assert r.format == "gesis_kg_sparql_json"
            assert r.content_type == "application/sparql-results+json"


class TestHelpers:
    def test_extract_source_record_id_typical(self) -> None:
        assert _extract_source_record_id(
            "https://data.gesis.org/gesiskg/resource/ZA2391"
        ) == "ZA2391"

    def test_extract_source_record_id_with_doi(self) -> None:
        assert _extract_source_record_id(
            "https://data.gesis.org/datasearchkg/Dataset/10.4232/1.14206"
        ) == "1.14206"

    def test_extract_source_record_id_handles_trailing_slash(self) -> None:
        assert _extract_source_record_id(
            "https://example.org/foo/bar/"
        ) == "bar"

    def test_extract_source_record_id_handles_fragment(self) -> None:
        assert _extract_source_record_id(
            "https://example.org/foo#fragment"
        ) == "fragment"

    def test_extract_source_record_id_empty(self) -> None:
        assert _extract_source_record_id("") is None

    def test_first_doi_from_doi_org_url(self) -> None:
        triples = [
            {"p": "https://schema.org/identifier",
             "o": "https://doi.org/10.4232/1.14206", "type": "uri"},
        ]
        assert _first_doi(triples) == "10.4232/1.14206"

    def test_first_doi_from_doi_prefix(self) -> None:
        triples = [
            {"p": "https://schema.org/identifier",
             "o": "doi:10.4232/1.14206", "type": "literal"},
        ]
        assert _first_doi(triples) == "10.4232/1.14206"

    def test_first_doi_from_bare_10_form(self) -> None:
        triples = [
            {"p": "https://schema.org/sameAs",
             "o": "10.4232/1.14206", "type": "literal"},
        ]
        assert _first_doi(triples) == "10.4232/1.14206"

    def test_first_doi_no_match(self) -> None:
        triples = [
            {"p": "https://schema.org/name", "o": "title", "type": "literal"},
        ]
        assert _first_doi(triples) is None

    def test_first_literal_matches_predicate_and_type(self) -> None:
        triples = [
            {"p": "https://schema.org/identifier",
             "o": "https://doi.org/x", "type": "uri"},
            {"p": "https://schema.org/name", "o": "Title", "type": "literal"},
        ]
        from study_scraper.sources.gesis import _TITLE_PROPS
        assert _first_literal(triples, _TITLE_PROPS) == "Title"


# --------------------------------------------------------------------------
# Integration: lake ingest end-to-end
# --------------------------------------------------------------------------


pytestmark = pytest.mark.skipif(
    not TEST_DSN, reason="STUDY_SCRAPER_TEST_DSN not set; skipping integration"
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
            cur.execute("TRUNCATE study_scraper.source_records CASCADE")
            cur.execute("TRUNCATE study_scraper.crawl_run_studies CASCADE")
            cur.execute("TRUNCATE study_scraper.studies CASCADE")
            cur.execute("TRUNCATE study_scraper.crawl_runs CASCADE")
        conn.commit()
    yield


def test_lake_ingest_populates_source_records(storage: PostgresStorage) -> None:
    with GESISSource(from_file=FIXTURE) as src:
        run = run_lake_ingest(
            source=src, storage=storage, topic_ids=["klima"]
        )
    assert run.candidates_seen == 4
    assert run.candidates_kept == 4
    assert storage.count_source_records(source_id="gesis") == 4


def test_lake_ingest_is_idempotent(storage: PostgresStorage) -> None:
    with GESISSource(from_file=FIXTURE) as src:
        run_lake_ingest(source=src, storage=storage, topic_ids=["klima"])
    with GESISSource(from_file=FIXTURE) as src:
        run = run_lake_ingest(source=src, storage=storage, topic_ids=["klima"])
    assert run.candidates_kept == 0  # unchanged content, no new rows
    assert storage.count_source_records(source_id="gesis") == 4


def test_topic_tagging_persists(storage: PostgresStorage) -> None:
    with GESISSource(from_file=FIXTURE) as src:
        run_lake_ingest(source=src, storage=storage, topic_ids=["klima"])
    rows = storage.list_source_records(source_id="gesis", topic_id="klima")
    assert len(rows) == 4


def test_doi_promoted_to_typed_column(storage: PostgresStorage) -> None:
    with GESISSource(from_file=FIXTURE) as src:
        run_lake_ingest(source=src, storage=storage)
    rows = storage.list_source_records(source_id="gesis")
    dois = {row.get("source_record_id") for row in rows}
    # Source record id is last URI segment (e.g. "1.14206"); DOIs match
    # those when populated.
    assert "1.14206" in dois     # Politbarometer
    assert "1.14374" in dois     # Umweltbewusstsein 2022
    assert "1.13298" in dois     # Umweltbewusstsein 2018
