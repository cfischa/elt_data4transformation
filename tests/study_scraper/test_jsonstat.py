"""JSON-stat 2.0 flattening (issue #86): pure decoder + the `eurostat-table`
CLI surface + storage accessor built on it."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

import pytest
from typer.testing import CliRunner

from study_scraper.cli import app
from study_scraper.jsonstat import flatten_jsonstat


TEST_DSN = os.environ.get("STUDY_SCRAPER_TEST_DSN")
EUROSTAT_FIXTURE = (
    Path(__file__).resolve().parent / "fixtures" / "eurostat" / "sample.json"
)


def _plain(text: str) -> str:
    """Strip ANSI escapes (rich forces terminal styling in CI)."""
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


# ---------------------------------------------------------------------------
# flatten_jsonstat — pure, no DB
# ---------------------------------------------------------------------------


def _small_payload(value):
    """2 dims (geo x2, time x3) — the value shapes under test are swapped
    in per-test so the dimension/category decoding stays fixed."""
    return {
        "id": ["geo", "time"],
        "size": [2, 3],
        "dimension": {
            "geo": {
                "category": {
                    "index": {"DE": 0, "FR": 1},
                    "label": {"DE": "Germany", "FR": "France"},
                }
            },
            "time": {
                "category": {
                    "index": {"2020": 0, "2021": 1, "2022": 2},
                    "label": {"2020": "2020", "2021": "2021", "2022": "2022"},
                }
            },
        },
        "value": value,
    }


class TestFlattenDenseArray:
    def test_decodes_every_cell_in_row_major_order(self) -> None:
        # time is the fastest-varying (last) dimension.
        payload = _small_payload([10, 11, 12, 20, 21, 22])
        rows = list(flatten_jsonstat(payload))
        assert len(rows) == 6
        by_key = {(r["geo"], r["time"]): r["value"] for r in rows}
        assert by_key[("Germany", "2020")] == 10
        assert by_key[("Germany", "2022")] == 12
        assert by_key[("France", "2020")] == 20
        assert by_key[("France", "2022")] == 22

    def test_skips_null_cells(self) -> None:
        payload = _small_payload([10, None, 12, 20, 21, 22])
        rows = list(flatten_jsonstat(payload))
        assert len(rows) == 5
        assert ("Germany", "2021") not in {(r["geo"], r["time"]) for r in rows}


class TestFlattenSparseObject:
    def test_decodes_only_present_indices(self) -> None:
        payload = _small_payload({"0": 10, "2": 12, "5": 22})
        rows = list(flatten_jsonstat(payload))
        by_key = {(r["geo"], r["time"]): r["value"] for r in rows}
        assert by_key == {
            ("Germany", "2020"): 10,
            ("Germany", "2022"): 12,
            ("France", "2022"): 22,
        }

    def test_ignores_non_numeric_keys(self) -> None:
        payload = _small_payload({"0": 10, "not-an-index": 99})
        rows = list(flatten_jsonstat(payload))
        assert len(rows) == 1
        assert rows[0]["value"] == 10


class TestFlattenEdgeCases:
    def test_missing_id_or_size_yields_nothing(self) -> None:
        assert list(flatten_jsonstat({})) == []
        assert list(flatten_jsonstat({"id": ["geo"], "size": []})) == []

    def test_out_of_range_index_is_skipped_not_raised(self) -> None:
        payload = _small_payload({"99": 1})
        assert list(flatten_jsonstat(payload)) == []

    def test_dimension_missing_from_dimension_dict_falls_back_to_raw_id(self) -> None:
        payload = {
            "id": ["geo", "unit"],
            "size": [2, 1],
            "dimension": {
                "geo": {
                    "category": {
                        "index": {"DE": 0, "FR": 1},
                        "label": {"DE": "Germany", "FR": "France"},
                    }
                }
                # "unit" intentionally omitted, like real abbreviated
                # Eurostat responses (see fixtures/eurostat/sample.json).
            },
            "value": [1, 2],
        }
        rows = list(flatten_jsonstat(payload))
        assert len(rows) == 2
        assert {r["unit"] for r in rows} == {"unit"}
        assert {r["geo"] for r in rows} == {"Germany", "France"}

    def test_category_as_list_uses_list_position_as_index(self) -> None:
        payload = {
            "id": ["geo"],
            "size": [2],
            "dimension": {
                "geo": {"category": {"index": ["DE", "FR"], "label": {}}}
            },
            "value": [1, 2],
        }
        rows = list(flatten_jsonstat(payload))
        by_geo = {r["geo"]: r["value"] for r in rows}
        # No label map -> falls back to the raw category id.
        assert by_geo == {"DE": 1, "FR": 2}


class TestFlattenRealEurostatFixture:
    """The abbreviated env_air_gge fixture used by EurostatSource's own
    tests — confirms the decoder handles a real (if trimmed) payload with
    src_crf/unit dimensions omitted from `dimension` entirely."""

    def test_decodes_known_linear_indices(self) -> None:
        raw = json.loads(EUROSTAT_FIXTURE.read_text(encoding="utf-8"))
        payload = raw["datasets"][0]["payload"]
        rows = list(flatten_jsonstat(payload))
        assert len(rows) == 5
        first = rows[0]
        assert first["freq"] == "Annual"
        assert first["airpol"] == "Greenhouse gases (CO2, N2O in CO2 equivalent, CH4 in CO2 equivalent)"
        assert first["geo"] == "European Union - 27 countries (from 2020)"
        assert first["time"] == "1990"
        assert first["value"] == 5651.06
        last = rows[-1]
        assert last["time"] == "2022"
        assert last["value"] == 1228.84


# ---------------------------------------------------------------------------
# CLI registration
# ---------------------------------------------------------------------------


def test_cli_registers_eurostat_table_command() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["eurostat-table", "--help"])
    assert result.exit_code == 0
    output = _plain(result.output)
    assert "--code" in output
    assert "--limit" in output


# ---------------------------------------------------------------------------
# Integration (real Postgres)
# ---------------------------------------------------------------------------

pytestmark_integration = pytest.mark.skipif(
    not TEST_DSN, reason="STUDY_SCRAPER_TEST_DSN not set; skipping integration"
)


@pytest.fixture(scope="module")
def storage():
    from study_scraper.storage import PostgresStorage
    assert TEST_DSN is not None
    store = PostgresStorage(TEST_DSN)
    store.migrate()
    return store


def _clean(storage) -> None:
    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE study_scraper.claims CASCADE")
            cur.execute("TRUNCATE study_scraper.source_records CASCADE")
            cur.execute("TRUNCATE study_scraper.crawl_runs CASCADE")
        conn.commit()


@pytestmark_integration
def test_get_source_record_payload_returns_latest_kept_payload(storage) -> None:
    from study_scraper.sources.eurostat import EurostatSource
    from study_scraper.ingest import run_lake_ingest

    _clean(storage)
    with EurostatSource(from_file=EUROSTAT_FIXTURE) as src:
        run_lake_ingest(source=src, storage=storage)

    payload = storage.get_source_record_payload(
        source_id="eurostat", source_record_id="env_air_gge"
    )
    assert payload is not None
    assert payload["id"] == ["freq", "airpol", "src_crf", "unit", "geo", "time"]

    assert storage.get_source_record_payload(
        source_id="eurostat", source_record_id="does-not-exist"
    ) is None


@pytestmark_integration
def test_eurostat_table_cli_prints_flattened_rows(storage, monkeypatch) -> None:
    from study_scraper.sources.eurostat import EurostatSource
    from study_scraper.ingest import run_lake_ingest

    _clean(storage)
    with EurostatSource(from_file=EUROSTAT_FIXTURE) as src:
        run_lake_ingest(source=src, storage=storage)

    import study_scraper.config as _cfg
    monkeypatch.setattr(_cfg, "_settings", None)
    monkeypatch.setenv("POSTGRES_URL", TEST_DSN)

    runner = CliRunner()
    result = runner.invoke(app, ["eurostat-table", "--code", "env_air_gge"])
    assert result.exit_code == 0, result.output
    assert "5651.06" in result.output
    assert "geo=" in result.output
