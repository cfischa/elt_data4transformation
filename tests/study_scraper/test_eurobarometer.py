"""Tests for the Eurobarometer lake source (lake-style, A14 / issue #35).

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
from study_scraper.sources.eurobarometer import (
    EurobarometerSource,
    _extract_source_record_id,
    _first_doi,
    _first_literal,
)
from study_scraper.storage import PostgresStorage


TEST_DSN = os.environ.get("STUDY_SCRAPER_TEST_DSN")
FIXTURE = (
    Path(__file__).resolve().parent
    / "fixtures" / "eurobarometer" / "sample.json"
)


# --------------------------------------------------------------------------
# Pure unit tests (always run)
# --------------------------------------------------------------------------


def _records():
    with EurobarometerSource(from_file=FIXTURE) as src:
        return list(
            src.iter_records(run_id="00000000-0000-0000-0000-000000000000")
        )


class TestEurobarometerParser:
    def test_yields_one_record_per_dataset_uri(self) -> None:
        # 2 climate/migration waves + 1 off-topic wave (negative control).
        assert len(_records()) == 3

    def test_canonical_url_is_kg_uri(self) -> None:
        urls = {r.canonical_url for r in _records()}
        assert (
            "https://data.gesis.org/datasearchkg/Dataset/10.4232/1.90001"
            in urls
        )

    def test_source_record_id_is_last_uri_segment(self) -> None:
        climate = next(
            r for r in _records() if r.canonical_url.endswith("10.4232/1.90001")
        )
        assert climate.source_record_id == "1.90001"

    def test_doi_extracted_from_schema_identifier(self) -> None:
        climate = next(
            r for r in _records() if r.canonical_url.endswith("10.4232/1.90001")
        )
        assert climate.doi == "10.4232/1.90001"

    def test_payload_preserves_all_triples(self) -> None:
        climate = next(
            r for r in _records() if r.canonical_url.endswith("10.4232/1.90001")
        )
        triples = climate.payload["triples"]
        assert len(triples) >= 10
        sorted_again = sorted(
            triples, key=lambda t: (t["p"], str(t["o"]))
        )
        assert triples == sorted_again

    def test_title_hint_propagated_to_provenance(self) -> None:
        climate = next(
            r for r in _records() if r.canonical_url.endswith("10.4232/1.90001")
        )
        assert "Eurobarometer" in (climate.provenance.get("title_hint") or "")

    def test_license_value_captured(self) -> None:
        climate = next(
            r for r in _records() if r.canonical_url.endswith("10.4232/1.90001")
        )
        assert climate.license is not None
        assert "gesis.org" in climate.license

    def test_record_with_no_license_falls_back_to_default(self) -> None:
        digital = next(
            r for r in _records() if r.canonical_url.endswith("10.4232/1.90003")
        )
        # Fixture deliberately omits license -> default applies.
        assert digital.license == "GESIS terms of use (Eurobarometer data archive)"

    def test_content_hash_deterministic(self) -> None:
        a = _records()
        b = _records()
        for ra, rb in zip(a, b):
            assert ra.content_hash == rb.content_hash

    def test_limit_truncates(self) -> None:
        with EurobarometerSource(from_file=FIXTURE) as src:
            recs = list(
                src.iter_records(
                    run_id="00000000-0000-0000-0000-000000000000",
                    limit=2,
                )
            )
        assert len(recs) == 2

    def test_record_format(self) -> None:
        for r in _records():
            assert r.format == "eurobarometer_kg_sparql_json"
            assert r.content_type == "application/sparql-results+json"


class TestHelpers:
    def test_extract_source_record_id_typical(self) -> None:
        assert _extract_source_record_id(
            "https://data.gesis.org/gesiskg/resource/ZA9001"
        ) == "ZA9001"

    def test_extract_source_record_id_with_doi(self) -> None:
        assert _extract_source_record_id(
            "https://data.gesis.org/datasearchkg/Dataset/10.4232/1.90001"
        ) == "1.90001"

    def test_first_doi_from_doi_org_url(self) -> None:
        triples = [
            {"p": "https://schema.org/identifier",
             "o": "https://doi.org/10.4232/1.90001", "type": "uri"},
        ]
        assert _first_doi(triples) == "10.4232/1.90001"

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
        from study_scraper.sources.eurobarometer import _TITLE_PROPS
        assert _first_literal(triples, _TITLE_PROPS) == "Title"


# --------------------------------------------------------------------------
# Integration: lake ingest end-to-end
#
# NOTE: the marker is scoped to this *class* (not a bare module-level
# `pytestmark`, which is a known pytest gotcha -- it would apply to
# every test in the module, silently skipping the pure parser tests
# above too, even without STUDY_SCRAPER_TEST_DSN). test_gesis.py and
# test_eurostat.py use the module-level form and appear to have the
# same latent issue; out of scope to fix here since this PR only adds
# the eurobarometer source.
# --------------------------------------------------------------------------


class TestLakeIngestIntegration:
    pytestmark = pytest.mark.skipif(
        not TEST_DSN,
        reason="STUDY_SCRAPER_TEST_DSN not set; skipping integration",
    )

    @pytest.fixture()
    def storage(self) -> PostgresStorage:
        assert TEST_DSN is not None
        store = PostgresStorage(TEST_DSN)
        store.migrate()
        return store

    @pytest.fixture(autouse=True)
    def _clean(self, storage: PostgresStorage) -> Iterator[None]:
        with storage.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE study_scraper.claims CASCADE")
                cur.execute("TRUNCATE study_scraper.source_records CASCADE")
                cur.execute("TRUNCATE study_scraper.crawl_run_studies CASCADE")
                cur.execute("TRUNCATE study_scraper.studies CASCADE")
                cur.execute("TRUNCATE study_scraper.crawl_runs CASCADE")
            conn.commit()
        yield

    def test_lake_ingest_populates_source_records(
        self, storage: PostgresStorage
    ) -> None:
        with EurobarometerSource(from_file=FIXTURE) as src:
            run = run_lake_ingest(
                source=src, storage=storage, topic_ids=["klima"]
            )
        assert run.candidates_seen == 3
        assert run.candidates_kept == 3
        assert storage.count_source_records(source_id="eurobarometer") == 3

    def test_lake_ingest_is_idempotent(self, storage: PostgresStorage) -> None:
        with EurobarometerSource(from_file=FIXTURE) as src:
            run_lake_ingest(source=src, storage=storage, topic_ids=["klima"])
        with EurobarometerSource(from_file=FIXTURE) as src:
            run = run_lake_ingest(
                source=src, storage=storage, topic_ids=["klima"]
            )
        assert run.candidates_kept == 0  # unchanged content, no new rows
        assert storage.count_source_records(source_id="eurobarometer") == 3

    def test_topic_tagging_persists(self, storage: PostgresStorage) -> None:
        with EurobarometerSource(from_file=FIXTURE) as src:
            run_lake_ingest(source=src, storage=storage, topic_ids=["klima"])
        rows = storage.list_source_records(
            source_id="eurobarometer", topic_id="klima"
        )
        assert len(rows) == 3

    def test_doi_promoted_to_typed_column(self, storage: PostgresStorage) -> None:
        with EurobarometerSource(from_file=FIXTURE) as src:
            run_lake_ingest(source=src, storage=storage)
        rows = storage.list_source_records(source_id="eurobarometer")
        dois = {row.get("source_record_id") for row in rows}
        assert "1.90001" in dois
        assert "1.90002" in dois
        assert "1.90003" in dois
