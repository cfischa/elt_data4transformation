"""Tests for the BMAS Rentenbestandsstatistik lake source (A14, #137)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator

import httpx
import pytest

from study_scraper.ingest import run_lake_ingest
from study_scraper.sources.bmas import BMASSource
from study_scraper.storage import PostgresStorage


TEST_DSN = os.environ.get("STUDY_SCRAPER_TEST_DSN")
FIXTURE = Path(__file__).resolve().parent / "fixtures" / "bmas" / "sample.json"
RUN_ID = "00000000-0000-0000-0000-000000000000"


# --------------------------------------------------------------------------
# Pure unit tests: fixture parser (always run)
# --------------------------------------------------------------------------


def _records():
    with BMASSource(from_file=FIXTURE) as src:
        return list(src.iter_records(run_id=RUN_ID))


class TestBMASParser:
    def test_yields_one_record_per_file(self) -> None:
        assert len(_records()) == 2

    def test_source_record_id_is_year_and_filename(self) -> None:
        ids = {r.source_record_id for r in _records()}
        assert "2024:rentenbestandsstatistik-07-2024-01-csv.csv" in ids
        assert "2024:rentenbestandsstatistik-07-2024-02-csv.csv" in ids

    def test_canonical_url_strips_blob_query(self) -> None:
        first = _records()[0]
        assert "__blob" not in first.canonical_url
        assert first.canonical_url == (
            "https://www.bmas.de/SharedDocs/Downloads/DE/Rente/"
            "rentenbestandsstatistik-07-2024-01-csv.csv"
        )

    def test_payload_parses_semicolon_csv_grid(self) -> None:
        first = _records()[0]
        rows = first.payload["rows"]
        assert first.payload["filename"] == "rentenbestandsstatistik-07-2024-01-csv.csv"
        assert first.payload["row_count"] == len(rows)
        header_row = rows[5]
        assert header_row[0] == "Versicherungszweig"
        assert header_row[1] == "Geschlecht"
        data_row = rows[6]
        assert data_row[0] == "Allgemeine Rentenversicherung"
        assert data_row[1] == "Männer"
        assert data_row[4] == "8620833"

    def test_umlauts_decode_correctly(self) -> None:
        first = _records()[0]
        assert "Erwerbsfähigkeit" in first.payload["rows"][5][5]

    def test_license_is_none_not_guessed(self) -> None:
        for record in _records():
            assert record.license is None

    def test_record_format_and_content_type(self) -> None:
        for record in _records():
            assert record.format == "bmas_rentenbestandsstatistik_csv"
            assert record.content_type == "text/csv"

    def test_provenance_captures_year_and_urls(self) -> None:
        first = _records()[0]
        assert first.provenance["year"] == "2024"
        assert first.provenance["filename"] == (
            "rentenbestandsstatistik-07-2024-01-csv.csv"
        )
        assert "__blob" in first.provenance["download_url"]

    def test_content_hash_deterministic(self) -> None:
        a = _records()
        b = _records()
        for ra, rb in zip(a, b):
            assert ra.content_hash == rb.content_hash

    def test_limit_truncates(self) -> None:
        with BMASSource(from_file=FIXTURE) as src:
            recs = list(src.iter_records(run_id=RUN_ID, limit=1))
        assert len(recs) == 1


# --------------------------------------------------------------------------
# Live-fetch behaviour: index-page link discovery, mocked transport
# --------------------------------------------------------------------------

_INDEX_HTML = """
<html><body>
<ul>
<li><a href="/SharedDocs/Downloads/DE/Rente/rentenbestandsstatistik-07-2024-xlsx.xlsx?__blob=publicationFile&v=2">XLSX</a></li>
<li><a href="/SharedDocs/Downloads/DE/Rente/rentenbestandsstatistik-07-2024-01-csv.csv?__blob=publicationFile&v=2">CSV 1</a></li>
<li><a href="/SharedDocs/Downloads/DE/Rente/rentenbestandsstatistik-07-2024-02-csv.csv?__blob=publicationFile&v=2">CSV 2</a></li>
</ul>
</body></html>
"""

_CSV_BODY = (
    "Titel;;\r\nQuelle: BMAS;;\r\n"
    "Versicherungszweig;Geschlecht;Anzahl\r\n"
    "Allgemeine Rentenversicherung;Männer;123\r\n"
).encode("latin-1")


def _client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_live_mode_discovers_only_csv_links_and_skips_xlsx() -> None:
    requested = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        if str(request.url).endswith(".html"):
            return httpx.Response(200, text=_INDEX_HTML)
        return httpx.Response(200, content=_CSV_BODY)

    with BMASSource(years=["2024"], client=_client(handler)) as src:
        records = list(src.iter_records(run_id=RUN_ID))

    assert len(records) == 2
    assert not any("xlsx" in url for url in requested)
    filenames = {r.provenance["filename"] for r in records}
    assert filenames == {
        "rentenbestandsstatistik-07-2024-01-csv.csv",
        "rentenbestandsstatistik-07-2024-02-csv.csv",
    }


def test_live_mode_decodes_latin1_body() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url).endswith(".html"):
            return httpx.Response(200, text=_INDEX_HTML)
        return httpx.Response(200, content=_CSV_BODY)

    with BMASSource(years=["2024"], client=_client(handler)) as src:
        records = list(src.iter_records(run_id=RUN_ID))

    assert records[0].payload["rows"][3][1] == "Männer"


def test_live_mode_without_years_uses_default() -> None:
    requested_years = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested_years.append(str(request.url))
        return httpx.Response(200, text="<html><body></body></html>")

    with BMASSource(client=_client(handler)) as src:
        list(src.iter_records(run_id=RUN_ID))

    assert any("rentenbestandsstatistik-2024.html" in u for u in requested_years)


# --------------------------------------------------------------------------
# Integration: lake ingest end-to-end
#
# Scoped to this class, not a module-level pytestmark (see
# test_eurobarometer.py's note on the same pytest gotcha).
# --------------------------------------------------------------------------


class TestLakeIngestIntegration:
    pytestmark = pytest.mark.skipif(
        not TEST_DSN,
        reason="STUDY_SCRAPER_TEST_DSN not set; skipping integration",
    )

    @pytest.fixture()
    def storage(self) -> PostgresStorage:
        assert TEST_DSN is not None
        store = PostgresStorage(TEST_DSN)
        store.migrate()
        return store

    @pytest.fixture(autouse=True)
    def _clean(self, storage: PostgresStorage) -> Iterator[None]:
        with storage.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE study_scraper.claims CASCADE")
                cur.execute("TRUNCATE study_scraper.source_records CASCADE")
                cur.execute("TRUNCATE study_scraper.crawl_run_studies CASCADE")
                cur.execute("TRUNCATE study_scraper.studies CASCADE")
                cur.execute("TRUNCATE study_scraper.crawl_runs CASCADE")
            conn.commit()
        yield

    def test_lake_ingest_populates_source_records(
        self, storage: PostgresStorage
    ) -> None:
        with BMASSource(from_file=FIXTURE) as src:
            run = run_lake_ingest(source=src, storage=storage, topic_ids=["rente"])
        assert run.candidates_seen == 2
        assert run.candidates_kept == 2
        assert storage.count_source_records(source_id="bmas") == 2

    def test_lake_ingest_is_idempotent(self, storage: PostgresStorage) -> None:
        with BMASSource(from_file=FIXTURE) as src:
            run_lake_ingest(source=src, storage=storage, topic_ids=["rente"])
        with BMASSource(from_file=FIXTURE) as src:
            run = run_lake_ingest(source=src, storage=storage, topic_ids=["rente"])
        assert run.candidates_kept == 0
        assert storage.count_source_records(source_id="bmas") == 2
