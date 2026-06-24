"""Tests for the lake-style source_records path + DAWUM source.

Pure unit tests for the model + parser; integration tests against real
Postgres exercise the storage + views (skipped without
STUDY_SCRAPER_TEST_DSN).
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import pytest

from study_scraper.ingest import run_lake_ingest
from study_scraper.models import SourceRecord, source_record_id_for
from study_scraper.sources.dawum import DAWUMSource
from study_scraper.storage import PostgresStorage


TEST_DSN = os.environ.get("STUDY_SCRAPER_TEST_DSN")
FIXTURE = (
    Path(__file__).resolve().parent / "fixtures" / "dawum" / "sample.json"
)


# --------------------------------------------------------------------------
# SourceRecord model unit tests
# --------------------------------------------------------------------------


class TestSourceRecordModel:
    def test_id_is_deterministic(self) -> None:
        a = source_record_id_for("dawum", "https://dawum.de/Bundestag/1001")
        b = source_record_id_for("dawum", "https://dawum.de/Bundestag/1001")
        assert a == b
        assert len(a) == 64

    def test_id_differs_across_sources_for_same_url(self) -> None:
        u = "https://example.org/x"
        assert source_record_id_for("dawum", u) != source_record_id_for("openalex", u)

    def test_build_requires_exactly_one_payload(self) -> None:
        kw = dict(
            source_id="x",
            canonical_url="https://x/y",
            format="dummy",
            content_hash="h",
            fetched_at=datetime.now(timezone.utc),
            discovery_run_id="00000000-0000-0000-0000-000000000000",
        )
        with pytest.raises(ValueError, match="exactly one"):
            SourceRecord.build(**kw)  # neither
        with pytest.raises(ValueError, match="exactly one"):
            SourceRecord.build(payload={"a": 1}, payload_uri="s3://x", **kw)  # both
        # Either alone is fine.
        ok1 = SourceRecord.build(payload={"a": 1}, **kw)
        ok2 = SourceRecord.build(payload_uri="s3://x", **kw)
        assert ok1.payload == {"a": 1}
        assert ok2.payload_uri == "s3://x"

    def test_naive_datetime_made_utc(self) -> None:
        rec = SourceRecord.build(
            source_id="x",
            canonical_url="https://x/y",
            format="dummy",
            content_hash="h",
            fetched_at=datetime(2026, 5, 28),  # naive
            discovery_run_id="00000000-0000-0000-0000-000000000000",
            payload={"a": 1},
        )
        assert rec.fetched_at.tzinfo is timezone.utc


# --------------------------------------------------------------------------
# DAWUM parser
# --------------------------------------------------------------------------


def _records():
    with DAWUMSource(from_file=FIXTURE) as src:
        return list(src.iter_records(run_id="00000000-0000-0000-0000-000000000000"))


class TestDAWUMParser:
    def test_one_record_per_survey(self) -> None:
        assert len(_records()) == 5

    def test_payload_denormalised(self) -> None:
        recs = _records()
        bundestag = next(r for r in recs if r.source_record_id == "1001")
        p = bundestag.payload
        # institute / tasker / parliament / method all resolved to labels
        assert p["institute"]["name"] == "Forsa"
        assert p["tasker"]["name"] == "RTL"
        assert p["parliament"]["name"] == "Bundestag"
        assert p["parliament"]["shortcut"] == "Bundestag"
        assert p["method"]["name"] == "Online"
        # results carries party_name + numeric percentage
        union = next(r for r in p["results"] if r["party_id"] == "1")
        assert union["party_name"] == "CDU/CSU"
        assert union["percentage"] == 30.0

    def test_state_parliament_uses_shortcut_in_url(self) -> None:
        recs = _records()
        bay = next(r for r in recs if r.source_record_id == "1004")
        assert bay.canonical_url == "https://dawum.de/Bayern/1004"

    def test_sample_size_parsed_int(self) -> None:
        recs = _records()
        for r in recs:
            assert isinstance(r.payload["sample_size"], int)
            assert r.payload["sample_size"] > 0

    def test_license_recorded(self) -> None:
        for r in _records():
            assert r.license == "ODC-ODbL"

    def test_content_hash_deterministic(self) -> None:
        a = _records()
        b = _records()
        for ra, rb in zip(a, b):
            assert ra.content_hash == rb.content_hash

    def test_limit(self) -> None:
        with DAWUMSource(from_file=FIXTURE) as src:
            recs = list(src.iter_records(
                run_id="00000000-0000-0000-0000-000000000000", limit=2
            ))
        assert len(recs) == 2


# --------------------------------------------------------------------------
# End-to-end against real Postgres
# --------------------------------------------------------------------------


pytestmark = pytest.mark.skipif(
    not TEST_DSN, reason="STUDY_SCRAPER_TEST_DSN not set; skipping lake integration"
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
    with DAWUMSource(from_file=FIXTURE) as src:
        run = run_lake_ingest(source=src, storage=storage)
    assert run.candidates_seen == 5
    assert run.candidates_kept == 5
    assert run.errors == 0
    assert storage.count_source_records(source_id="dawum") == 5


def test_lake_ingest_is_idempotent(storage: PostgresStorage) -> None:
    with DAWUMSource(from_file=FIXTURE) as src:
        run_lake_ingest(source=src, storage=storage)
    with DAWUMSource(from_file=FIXTURE) as src:
        run = run_lake_ingest(source=src, storage=storage)
    # Same content -> no new rows on second run.
    assert run.candidates_kept == 0
    assert storage.count_source_records(source_id="dawum") == 5


def test_optional_topic_tagging(storage: PostgresStorage) -> None:
    with DAWUMSource(from_file=FIXTURE) as src:
        run_lake_ingest(
            source=src, storage=storage, topic_ids=["klima"]
        )
    rows = storage.list_source_records(source_id="dawum", topic_id="klima")
    assert len(rows) == 5


def test_dawum_polls_view_returns_typed_columns(storage: PostgresStorage) -> None:
    """The lake-then-view pattern: raw payload in source_records,
    typed columns via SQL view -- no per-source table."""
    with DAWUMSource(from_file=FIXTURE) as src:
        run_lake_ingest(source=src, storage=storage)
    rows = storage.query_view("dawum_polls", limit=10)
    assert len(rows) == 5
    # Typed projections
    sample = rows[0]
    assert "poll_date" in sample
    assert "institute_name" in sample
    assert "sample_size" in sample
    # Postgres returns ::date as datetime.date in psycopg
    from datetime import date as _date
    assert isinstance(sample["poll_date"], _date)
    assert isinstance(sample["sample_size"], int)


def test_dawum_poll_results_view_explodes_parties(
    storage: PostgresStorage,
) -> None:
    with DAWUMSource(from_file=FIXTURE) as src:
        run_lake_ingest(source=src, storage=storage)
    rows = storage.query_view("dawum_poll_results", limit=100)
    # 5 polls × (7 or 8 parties each); >= 35 total rows expected.
    assert len(rows) >= 35
    # Each row carries a typed percentage.
    for r in rows:
        assert r["percentage"] is None or isinstance(r["percentage"], float) or hasattr(r["percentage"], "is_finite")
    # Union (party_id=1) should be present.
    unions = [r for r in rows if r["party_id"] == "1"]
    assert unions
    assert all(r["party_name"] == "CDU/CSU" for r in unions)
