"""Tests for the GovData.de (CKAN) lake source (A14 / issue #64).

Pure-Python unit tests for the parser + topic-query helper; integration
tests against real Postgres exercise the lake ingest path (skipped
without STUDY_SCRAPER_TEST_DSN).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator

import pytest

from study_scraper.ingest import run_lake_ingest
from study_scraper.sources.govdata import GovDataSource, topic_query_terms
from study_scraper.storage import PostgresStorage
from study_scraper.topics import Topic, TopicLocale


TEST_DSN = os.environ.get("STUDY_SCRAPER_TEST_DSN")
FIXTURE = (
    Path(__file__).resolve().parent / "fixtures" / "govdata" / "sample.json"
)


# --------------------------------------------------------------------------
# Pure unit tests (always run)
# --------------------------------------------------------------------------


def _records():
    with GovDataSource(from_file=FIXTURE) as src:
        return list(
            src.iter_records(run_id="00000000-0000-0000-0000-000000000000")
        )


class TestGovDataParser:
    def test_yields_one_record_per_dataset(self) -> None:
        assert len(_records()) == 3

    def test_canonical_url_uses_dataset_name(self) -> None:
        urls = {r.canonical_url for r in _records()}
        assert (
            "https://www.govdata.de/dataset/steueraufkommen-nach-steuerart-2024"
            in urls
        )

    def test_source_record_id_is_dataset_id(self) -> None:
        tax = next(
            r for r in _records()
            if r.canonical_url.endswith("steueraufkommen-nach-steuerart-2024")
        )
        assert tax.source_record_id == "8f2b6f1a-1111-4a11-9a11-111111111111"

    def test_license_captured_per_record_not_hardcoded(self) -> None:
        tax = next(
            r for r in _records()
            if r.canonical_url.endswith("steueraufkommen-nach-steuerart-2024")
        )
        co2 = next(
            r for r in _records()
            if r.canonical_url.endswith("co2-emissionen-bundeslaender-2023")
        )
        assert tax.license == "Datenlizenz Deutschland – Namensnennung – Version 2.0"
        assert co2.license == "Creative Commons CC Zero (CC0)"
        assert tax.license != co2.license

    def test_record_with_no_license_falls_back_to_default(self) -> None:
        musterstadt = next(
            r for r in _records()
            if r.canonical_url.endswith("kommunale-haushaltsdaten-musterstadt")
        )
        assert musterstadt.license == "Data License Germany 2.0 (dl-de/by-2-0)"

    def test_payload_preserves_full_dataset(self) -> None:
        tax = next(
            r for r in _records()
            if r.canonical_url.endswith("steueraufkommen-nach-steuerart-2024")
        )
        assert tax.payload["title"] == "Steueraufkommen nach Steuerart 2024"
        assert tax.payload["resources"][0]["format"] == "CSV"

    def test_publisher_hint_from_organization(self) -> None:
        tax = next(
            r for r in _records()
            if r.canonical_url.endswith("steueraufkommen-nach-steuerart-2024")
        )
        assert tax.provenance["publisher_hint"] == "Statistisches Bundesamt (Destatis)"

    def test_content_hash_deterministic(self) -> None:
        a = _records()
        b = _records()
        for ra, rb in zip(a, b):
            assert ra.content_hash == rb.content_hash

    def test_limit_truncates(self) -> None:
        with GovDataSource(from_file=FIXTURE) as src:
            recs = list(
                src.iter_records(
                    run_id="00000000-0000-0000-0000-000000000000",
                    limit=2,
                )
            )
        assert len(recs) == 2

    def test_record_format(self) -> None:
        for r in _records():
            assert r.format == "govdata_ckan_package"
            assert r.content_type == "application/json"

    def test_live_mode_without_queries_raises(self) -> None:
        with GovDataSource() as src:
            with pytest.raises(RuntimeError):
                list(src.iter_records(run_id="x"))


class TestTopicQueryTerms:
    def _topic(self) -> Topic:
        return Topic(
            id="steuern",
            locales={
                "de": TopicLocale(
                    name="Steuern",
                    description="...",
                    include_keywords=["Erbschaftssteuer", "Einkommensteuer"],
                    synonyms=["Steuerpolitik"],
                ),
                "en": TopicLocale(
                    name="Taxes",
                    description="...",
                    include_keywords=["inheritance tax"],
                    synonyms=["tax policy"],
                ),
            },
        )

    def test_uses_include_keywords_not_synonyms(self) -> None:
        terms = topic_query_terms(self._topic())
        assert "Erbschaftssteuer" in terms
        assert "Steuerpolitik" not in terms
        assert "tax policy" not in terms

    def test_german_locale_first(self) -> None:
        terms = topic_query_terms(self._topic())
        assert terms[0] == "Erbschaftssteuer"

    def test_caps_at_max_terms(self) -> None:
        topic = Topic(
            id="many",
            locales={
                "de": TopicLocale(
                    name="Many",
                    description="...",
                    include_keywords=[f"kw{i}" for i in range(10)],
                ),
            },
        )
        assert len(topic_query_terms(topic, max_terms=3)) == 3

    def test_dedupes_case_insensitive(self) -> None:
        topic = Topic(
            id="dup",
            locales={
                "de": TopicLocale(
                    name="Dup",
                    description="...",
                    include_keywords=["Steuer", "steuer", "Steuer "],
                ),
            },
        )
        assert topic_query_terms(topic) == ["Steuer"]


# --------------------------------------------------------------------------
# Integration: lake ingest end-to-end
#
# Scoped to this class, not a module-level pytestmark (see
# test_eurobarometer.py's note on the same pytest gotcha).
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
        with GovDataSource(from_file=FIXTURE) as src:
            run = run_lake_ingest(
                source=src, storage=storage, topic_ids=["steuern"]
            )
        assert run.candidates_seen == 3
        assert run.candidates_kept == 3
        assert storage.count_source_records(source_id="govdata") == 3

    def test_lake_ingest_is_idempotent(self, storage: PostgresStorage) -> None:
        with GovDataSource(from_file=FIXTURE) as src:
            run_lake_ingest(source=src, storage=storage, topic_ids=["steuern"])
        with GovDataSource(from_file=FIXTURE) as src:
            run = run_lake_ingest(
                source=src, storage=storage, topic_ids=["steuern"]
            )
        assert run.candidates_kept == 0  # unchanged content, no new rows
        assert storage.count_source_records(source_id="govdata") == 3

    def test_topic_tagging_persists(self, storage: PostgresStorage) -> None:
        with GovDataSource(from_file=FIXTURE) as src:
            run_lake_ingest(source=src, storage=storage, topic_ids=["steuern"])
        rows = storage.list_source_records(
            source_id="govdata", topic_id="steuern"
        )
        assert len(rows) == 3
