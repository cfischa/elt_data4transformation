import json
from pathlib import Path

from elt.adapters.metadata import adapt_soep_metadata

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "data" / "soep"


def _load(name: str):
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def test_adapt_soep_metadata_produces_canonical_payload():
    summary = _load("indicators_page1.json")["data"][0]
    detail = _load("indicator_detail_income.json")["data"]

    dimension_dict = {
        dim["dimension"]: dim["categories"] for dim in detail["dimensions"]
    }
    row = {
        "slug": summary["slug"],
        "title": summary["title"],
        "description": summary["short_description"],
        "topics": [topic["title"] for topic in summary["topics"]],
        "dimensions": json.dumps(dimension_dict),
        "available_years": detail["available_years"],
        "last_updated_at": summary["last_updated_at"],
        "raw_summary": json.dumps(summary),
        "raw_detail": json.dumps(detail),
    }

    metadata = adapt_soep_metadata(row)

    assert metadata.source == "soep"
    assert metadata.dataset_id == "income-distribution"
    assert metadata.title == "Household income (median)"
    assert "Income and Living Conditions" in metadata.keywords
    assert any(var.startswith("sex:") for var in metadata.variables)
    assert metadata.latest_update and metadata.latest_update.year == 2024
    assert metadata.metadata_hash


def test_adapt_soep_metadata_handles_missing_fields():
    row = {
        "slug": "life-satisfaction",
        "title": "",
        "description": None,
        "topics": [],
        "dimensions": "{}",
        "available_years": [],
        "last_updated_at": "",
        "raw_summary": "{}",
        "raw_detail": "{}",
    }

    metadata = adapt_soep_metadata(row)
    assert metadata.title == "life-satisfaction"
    assert metadata.description
    assert metadata.keywords == []
    assert metadata.variables == []
    assert metadata.metadata_hash
