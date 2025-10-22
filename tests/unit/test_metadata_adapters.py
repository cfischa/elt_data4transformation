"""Unit tests for metadata adapters."""

from __future__ import annotations

import json
from pathlib import Path

from elt.adapters.metadata import adapt_eurostat_metadata

FIXTURES = Path(__file__).parent.parent / "fixtures" / "eurostat"


def test_adapt_eurostat_metadata_produces_canonical_payload():
    metadata_fixture = (FIXTURES / "dataset_metadata.json").read_text(encoding="utf-8")
    raw = json.loads(metadata_fixture)

    row = {
        "dataset_code": "nama_10_gdp",
        "title": "GDP and main components",
        "description": "Gross domestic product at market prices",
        "last_update": "2024-01-15T10:00:00Z",
        "keywords": ["gdp", "economy"],
        "themes": ["economy_and_finance"],
        "dimensions": ["geo", "time", "na_item"],
        "values_count": 2048,
        "raw_metadata": json.dumps(raw, ensure_ascii=False),
    }

    metadata = adapt_eurostat_metadata(row)

    assert metadata.source == "eurostat"
    assert metadata.dataset_id == "nama_10_gdp"
    assert "economy_and_finance" in metadata.keywords
    assert any(var.startswith("geo") for var in metadata.variables)
    assert metadata.latest_update.isoformat() == "2024-01-15T10:00:00+00:00"
    assert metadata.metadata_hash
