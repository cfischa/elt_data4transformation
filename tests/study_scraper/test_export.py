"""Tests for the open dataset export (pure via fake storage + one DB test)."""

from __future__ import annotations

import csv
import datetime
import json
import os

import pytest

from study_scraper.export import run_export


class FakeStorage:
    def list_attributions_for_export(self):
        return [
            {"question": "Stricter climate laws", "position": "support",
             "percentage": 62.0, "population": None, "confidence": 0.9,
             "model": "llm-v1", "sample_size": 1009,
             "publication_date": datetime.date(2021, 6, 1),
             "publisher": "Forsa", "title": "Klima-Umfrage",
             "source_id": "openalex",
             "canonical_url": "https://example.org/a",
             "topic_ids": ["klima", "energie"]},
        ]

    def list_studies_for_export(self):
        return [
            {"id": "a" * 64, "title": "Klima-Umfrage", "publisher": "Forsa",
             "publication_date": datetime.date(2021, 6, 1), "language": "de",
             "topic_ids": ["klima"], "has_quantitative_data": True,
             "source_id": "openalex",
             "canonical_url": "https://example.org/a"},
        ]


def test_export_writes_csvs_and_manifest(tmp_path) -> None:
    manifest = run_export(FakeStorage(), tmp_path / "ds")

    assert manifest["findings"] == 1 and manifest["studies"] == 1
    on_disk = json.loads((tmp_path / "ds" / "manifest.json").read_text())
    assert on_disk["files"] == ["findings.csv", "studies.csv"]
    assert "notes" in on_disk

    with (tmp_path / "ds" / "findings.csv").open() as fh:
        rows = list(csv.DictReader(fh))
    assert rows[0]["question"] == "Stricter climate laws"
    assert rows[0]["percentage"] == "62.0"
    assert rows[0]["publication_date"] == "2021-06-01"
    assert rows[0]["topic_ids"] == "klima|energie"   # list flattening

    with (tmp_path / "ds" / "studies.csv").open() as fh:
        srows = list(csv.DictReader(fh))
    assert srows[0]["canonical_url"] == "https://example.org/a"
    # Legal shape: no abstract column anywhere.
    assert "abstract" not in srows[0] and "abstract" not in rows[0]


TEST_DSN = os.environ.get("STUDY_SCRAPER_TEST_DSN")


@pytest.mark.skipif(not TEST_DSN, reason="STUDY_SCRAPER_TEST_DSN not set")
def test_export_storage_queries_run(tmp_path) -> None:
    from study_scraper.storage import PostgresStorage

    store = PostgresStorage(TEST_DSN)
    store.migrate()
    manifest = run_export(store, tmp_path / "ds")
    assert (tmp_path / "ds" / "findings.csv").exists()
    assert manifest["findings"] >= 0
