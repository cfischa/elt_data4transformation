"""Tests for the Eurostat lake source."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Iterator

import pytest

from study_scraper.ingest import run_lake_ingest
from study_scraper.sources.eurostat import EurostatSource
from study_scraper.storage import PostgresStorage


TEST_DSN = os.environ.get("STUDY_SCRAPER_TEST_DSN")
FIXTURE = (
    Path(__file__).resolve().parent / "fixtures" / "eurostat" / "sample.json"
)


# --------------------------------------------------------------------------
# Pure unit tests
# --------------------------------------------------------------------------


def _records():
    with EurostatSource(from_file=FIXTURE) as src:
        return list(
            src.iter_records(run_id="00000000-0000-0000-0000-000000000000")
        )


class TestEurostatParser:
    def test_yields_one_record_per_dataset(self) -> None:
        assert len(_records()) == 2

    def test_canonical_url_is_databrowser_view(self) -> None:
        ghg = next(r for r in _records() if r.source_record_id == "env_air_gge")
        assert (
            ghg.canonical_url
            == "https://ec.europa.eu/eurostat/databrowser/view/env_air_gge"
        )

    def test_source_record_id_is_dataset_code(self) -> None:
        ids = {r.source_record_id for r in _records()}
        assert ids == {"env_air_gge", "nrg_bal_s"}

    def test_payload_preserved_verbatim(self) -> None:
        ghg = next(r for r in _records() if r.source_record_id == "env_air_gge")
        # The JSON-stat payload arrives unchanged into the lake.
        assert ghg.payload["class"] == "dataset"
        assert "label" in ghg.payload
        assert "id" in ghg.payload
        assert "dimension" in ghg.payload
        # Round-trip: serializing again gives the same json bytes
        # (sort_keys=True) -- this is what fuels content_hash.
        a = json.dumps(ghg.payload, ensure_ascii=False, sort_keys=True)
        assert "EU27_2020" in a

    def test_license_and_format(self) -> None:
        for r in _records():
            assert r.license == "Eurostat re-use policy (CC BY 4.0)"
            assert r.format == "eurostat_jsonstat"
            assert r.content_type == "application/json"

    def test_provenance_carries_size_and_updated(self) -> None:
        ghg = next(r for r in _records() if r.source_record_id == "env_air_gge")
        assert ghg.provenance["code"] == "env_air_gge"
        assert ghg.provenance["title_hint"].startswith("Greenhouse gas")
        assert ghg.provenance["updated"] == "2025-04-23"
        assert isinstance(ghg.provenance["size"], list)
        assert len(ghg.provenance["size"]) >= 4

    def test_content_hash_deterministic(self) -> None:
        a = _records()
        b = _records()
        for ra, rb in zip(a, b):
            assert ra.content_hash == rb.content_hash

    def test_limit_truncates(self) -> None:
        with EurostatSource(from_file=FIXTURE) as src:
            recs = list(
                src.iter_records(
                    run_id="00000000-0000-0000-0000-000000000000",
                    limit=1,
                )
            )
        assert len(recs) == 1

    def test_live_mode_without_codes_raises(self, tmp_path) -> None:
        # No from_file, no codes => should error before any HTTP attempt.
        src = EurostatSource()
        with pytest.raises(RuntimeError, match="at least one dataset code"):
            list(src.iter_records(run_id="x"))

    def test_single_dataset_fixture_with_extension_id(self, tmp_path) -> None:
        """A raw JSON-stat fixture (single dataset) should also work."""
        path = tmp_path / "single.json"
        path.write_text(json.dumps({
            "class": "dataset",
            "label": "Single GHG",
            "updated": "2025-01-01",
            "extension": {"id": "env_air_gge"},
            "id": ["geo"], "size": [1],
            "dimension": {"geo": {"category": {"index": {"DE": 0}}}},
            "value": {"0": 1.23},
        }), encoding="utf-8")
        with EurostatSource(from_file=path) as src:
            recs = list(src.iter_records(run_id="x"))
        assert len(recs) == 1
        assert recs[0].source_record_id == "env_air_gge"


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


def test_eurostat_lake_ingest(storage: PostgresStorage) -> None:
    with EurostatSource(from_file=FIXTURE) as src:
        run = run_lake_ingest(
            source=src, storage=storage, topic_ids=["klima"]
        )
    assert run.candidates_seen == 2
    assert run.candidates_kept == 2
    assert storage.count_source_records(source_id="eurostat") == 2


def test_eurostat_ingest_is_idempotent(storage: PostgresStorage) -> None:
    with EurostatSource(from_file=FIXTURE) as src:
        run_lake_ingest(source=src, storage=storage)
    with EurostatSource(from_file=FIXTURE) as src:
        second = run_lake_ingest(source=src, storage=storage)
    assert second.candidates_kept == 0  # nothing new
    assert storage.count_source_records(source_id="eurostat") == 2
