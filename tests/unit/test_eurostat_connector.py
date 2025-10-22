"""Unit tests for the Eurostat connector."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Dict
from unittest.mock import AsyncMock

import pytest

from connectors.eurostat_connector import EurostatConnector


FIXTURES = Path(__file__).parent.parent / "fixtures" / "eurostat"


def load_fixture(name: str) -> Dict:
    path = FIXTURES / name
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def test_list_datasets_parses_response(monkeypatch):
    connector = EurostatConnector()
    payload = load_fixture("datasets_page.json")
    mock_get = AsyncMock(return_value=payload)
    monkeypatch.setattr(connector, "get", mock_get)

    async def scenario():
        result = await connector.list_datasets(page=2, page_size=2, search="gdp")

        assert result["page"] == payload["page"]
        assert result["page_size"] == payload["pageSize"]
        assert len(result["items"]) == 2

        first = result["items"][0]
        assert first["code"] == "nama_10_gdp"
        assert first["title"] == "GDP and main components"
        assert first["last_update"].isoformat() == "2024-01-15T10:00:00+00:00"
        assert "national_accounts" in first["themes"]

        mock_get.assert_awaited_once()
        params = mock_get.await_args.kwargs["params"]
        assert params["page"] == 2
        assert params["pageSize"] == 2
        assert params["search"] == "gdp"

    asyncio.run(scenario())


def test_fetch_dataset_expands_values(tmp_path, monkeypatch):
    connector = EurostatConnector()
    connector.persistence = connector.persistence.__class__(tmp_path)

    data_payload = load_fixture("dataset_data.json")
    metadata_payload = load_fixture("dataset_metadata.json")

    mock_get = AsyncMock(side_effect=[data_payload, metadata_payload])
    monkeypatch.setattr(connector, "get", mock_get)

    async def scenario():
        result = await connector.fetch_dataset(
            "nama_10_gdp", filters={"geo": ["DE"]}, time_range=("2020", "2021")
        )

        assert result["dataset_id"] == "nama_10_gdp"
        assert len(result["records"]) == len(data_payload["value"])
        first_record = result["records"][0]
        assert first_record["geo"] == "DE"
        assert first_record["time"] == "2020"
        assert first_record["na_item_label"].startswith("Gross domestic product")

        saved_path = Path(result["raw_path"])
        assert saved_path.exists()

        mock_get.assert_awaited()
        calls = mock_get.await_args_list
        assert calls[0].args[0] == "data/nama_10_gdp"
        assert calls[1].args[0] == "datasets/nama_10_gdp"

    asyncio.run(scenario())


def test_fetch_data_handles_multiple_entries(monkeypatch):
    connector = EurostatConnector()

    payload = load_fixture("dataset_data.json")
    metadata_payload = load_fixture("dataset_metadata.json")
    mock_get = AsyncMock(side_effect=[payload, metadata_payload, payload, metadata_payload])
    monkeypatch.setattr(connector, "get", mock_get)

    datasets = ["nama_10_gdp", {"dataset": "nama_10_gdp", "filters": {"geo": ["FR"]}}]
    results = []

    async def scenario():
        async for item in connector.fetch_data(datasets=datasets):
            results.append(item)

    asyncio.run(scenario())

    assert len(results) == 2
    assert all(result.get("records") for result in results)
    assert mock_get.await_count == 4

