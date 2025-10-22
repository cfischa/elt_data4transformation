import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from connectors.soep_connector import SOEPConnector

FIXTURE_DIR = Path(__file__).parent / "data" / "soep"


def load_json(name: str) -> Dict[str, Any]:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def test_list_indicators_handles_pagination(monkeypatch):
    page_one = load_json("indicators_page1.json")
    page_two = {"data": [], "links": {}, "meta": {"total_pages": 1}}
    responses = [page_one, page_two]

    async def fake_get(self, endpoint: str, params: Dict[str, Any] | None = None):
        assert endpoint == "indicators"
        return responses.pop(0)

    monkeypatch.setattr(SOEPConnector, "get", fake_get)

    connector = SOEPConnector()
    items = asyncio.run(connector.list_indicators())
    assert len(items) == 2
    slugs = {item["slug"] for item in items}
    assert {"income-distribution", "life-satisfaction"} == slugs


def test_fetch_metadata_merges_detail(monkeypatch):
    indicators = load_json("indicators_page1.json")
    detail = load_json("indicator_detail_income.json")

    async def fake_list(self, page_size=None, max_pages=None):  # noqa: ANN001
        return [indicators["data"][0]]

    async def fake_detail(self, slug: str):
        assert slug == "income-distribution"
        return detail["data"]

    monkeypatch.setattr(SOEPConnector, "list_indicators", fake_list)
    monkeypatch.setattr(SOEPConnector, "get_indicator_detail", fake_detail)

    connector = SOEPConnector()
    metadata = asyncio.run(connector.fetch_metadata())
    assert len(metadata) == 1
    record = metadata[0]
    assert record["slug"] == "income-distribution"
    assert record["title"] == "Household income (median)"
    assert "Income and Living Conditions" in record["topics"]
    assert "sex" in record["dimensions"]
    assert record["unit"].startswith("Euro")


def test_get_incremental_data_filters_years(monkeypatch):
    observations = load_json("observations_income.json")

    async def fake_stream(self, slug: str, per_page=None, **filters):  # noqa: ANN001
        assert slug == "income-distribution"
        for row in observations["data"]:
            yield row

    monkeypatch.setattr(SOEPConnector, "stream_observations", fake_stream)

    connector = SOEPConnector()
    since = datetime(2020, 1, 1)

    async def _collect() -> list:
        payloads = []
        async for payload in connector.get_incremental_data(
            since,
            datasets=["income-distribution"],
        ):
            payloads.append(payload)
        return payloads

    payloads = asyncio.run(_collect())
    assert len(payloads) == 1
    filtered = payloads[0]["observations"]
    years = {row["year"] for row in filtered}
    assert years == {2020, 2022}
