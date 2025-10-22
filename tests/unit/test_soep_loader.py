import json
from datetime import datetime
from unittest.mock import MagicMock, patch

from elt.loader_clickhouse import ClickHouseLoader


def _loader_instance() -> ClickHouseLoader:
    instance = ClickHouseLoader.__new__(ClickHouseLoader)
    instance.config = MagicMock()
    instance.logger = MagicMock()
    instance.client = MagicMock()
    return instance


def test_upsert_soep_metadata_prepares_payload():
    loader = _loader_instance()
    metadata = [
        {
            "slug": "income-distribution",
            "title": "Household income (median)",
            "subtitle": "Median equivalised household income",
            "short_description": "Median equivalised disposable household income in euros.",
            "topics": ["Income", "Inequality"],
            "dimensions": {"sex": [{"label": "Total"}]},
            "unit": "EUR",
            "source": "SOEP-Core",
            "methodology": "OECD-modified equivalence scale",
            "available_years": [2019, 2020, 2022],
            "last_updated_at": "2024-03-12T09:30:00Z",
            "raw_summary": {"slug": "income-distribution"},
            "raw_detail": {"details": True},
        }
    ]

    with (
        patch.object(loader, "create_soep_metadata_table") as create_table,
        patch.object(loader, "load_dicts", return_value=1) as load_dicts,
    ):
        rows_loaded = loader.upsert_soep_metadata(metadata)

    create_table.assert_called_once()
    load_dicts.assert_called_once()
    args, _ = load_dicts.call_args
    processed_row = args[0][0]
    assert args[1] == "raw.soep_metadata"
    assert processed_row["slug"] == "income-distribution"
    assert processed_row["topics"] == ["Income", "Inequality"]
    assert json.loads(processed_row["dimensions"]) == {"sex": [{"label": "Total"}]}
    assert processed_row["available_years"] == [2019, 2020, 2022]
    assert isinstance(processed_row["ingestion_time"], datetime)
    assert json.loads(processed_row["raw_detail"]) == {"details": True}
    assert rows_loaded == 1


def test_upsert_soep_metadata_handles_empty():
    loader = _loader_instance()
    with patch.object(loader, "create_soep_metadata_table") as create_table:
        assert loader.upsert_soep_metadata([]) == 0
    create_table.assert_not_called()
