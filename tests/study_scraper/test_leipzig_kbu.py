"""Tests for the Leipzig KBU CKAN lake source (issue #232)."""

from __future__ import annotations

from pathlib import Path

from study_scraper.sources.leipzig_kbu import LeipzigKBUSource

FIXTURE = (
    Path(__file__).resolve().parent / "fixtures" / "leipzig_kbu" / "sample.json"
)
RUN = "00000000-0000-0000-0000-000000000000"


def _records():
    with LeipzigKBUSource(from_file=FIXTURE) as src:
        return list(src.iter_records(run_id=RUN))


def test_unlicensed_dataset_is_skipped() -> None:
    names = {r.canonical_url.rsplit("/", 1)[-1] for r in _records()}
    assert names == {"buergerumfrage-2024", "buergerumfrage-2022"}


def test_records_carry_leipzig_identity_and_provenance() -> None:
    rec = next(r for r in _records() if r.canonical_url.endswith("-2024"))
    assert rec.source_id == "leipzig_kbu"
    assert rec.canonical_url == "https://opendata.leipzig.de/dataset/buergerumfrage-2024"
    assert rec.license == "Datenlizenz Deutschland – Namensnennung – Version 2.0"
    assert rec.provenance["fetch_source"] == "leipzig_ckan_package_search"
    assert rec.provenance["publisher_hint"] == "Stadt Leipzig"


def test_resource_listing_preserved_not_downloaded() -> None:
    rec = next(r for r in _records() if r.canonical_url.endswith("-2024"))
    assert rec.payload["resources"][0]["format"] == "CSV"


def test_live_defaults_to_buergerumfrage_query() -> None:
    src = LeipzigKBUSource()
    try:
        assert src._queries == ["Bürgerumfrage"]
        assert "opendata.leipzig.de" in src._base_url
    finally:
        src.close()
