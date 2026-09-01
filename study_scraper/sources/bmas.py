"""BMAS Rentenbestandsstatistik (pension-statistics) lake source (A14, #137).

BMAS (Bundesministerium für Arbeit und Soziales) publishes yearly pension
recipient/amount statistics as direct-download files, no login required:

    https://www.bmas.de/DE/Service/Statistiken-Open-Data/
    Rentenbestandsstatistik/rentenbestandsstatistik-{year}.html

Since the 2021-07-01 revision each year page lists the combined Excel file
plus, for 2021-2024 (confirmed live, scouted 2026-08-31), 24 individual
CSV tables covering the same reporting date. v1 only ingests the CSV
tables: they need no new dependency (stdlib `csv` + the already-vendored
`beautifulsoup4` for link discovery), whereas the yearly Excel file would
need `openpyxl`, which isn't in `pyproject.toml` and adding it is outside
the developer agent's scope (root `pyproject.toml` isn't under
`study_scraper/**`). See DECISIONS.md.

Modes (mirrors DAWUM / Eurostat / GESIS / GovData):

  * `from_file=<path>` -- reads one JSON fixture: `{"files": [{"year":
    "2024", "filename": "...", "url": "...", "csv_text": "..."}, ...]}`.
    Used by tests and no-network environments.
  * Live (default) -- for each requested year, fetches the year's index
    page, discovers every `.csv` download link on it, and fetches each
    file. Both modes share `_parse_csv_bytes`/`_build_record`.

Payload is the parsed CSV grid (list of rows, each a list of cell
strings) preserved as-is -- BMAS's CSVs open with several title/source
rows before the header row, so there's no single well-defined "the"
header to project out; per A14 that projection is a later SQL view's job.

License: not stated anywhere on the page (checked live 2026-08-31 --
footer is a bare "© BMAS", no CC/Datenlizenz Deutschland marking, unlike
GovData's per-dataset `license_title`). Recorded as `license=None` rather
than guessing, per #137's ask.

Encoding: the response's `content-type` header claims `charset=UTF-8`,
but that's wrong -- verified live (2026-08-31) that the bytes are actually
ISO-8859-1 (German umlauts fail to decode as UTF-8, decode cleanly as
Latin-1).
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple
from urllib.parse import urljoin, urlsplit, urlunsplit

import httpx
from bs4 import BeautifulSoup

from study_scraper.http import get_with_retry
from study_scraper.models import SourceRecord


LOGGER = logging.getLogger(__name__)

BASE_URL = "https://www.bmas.de"
INDEX_URL_TEMPLATE = (
    BASE_URL
    + "/DE/Service/Statistiken-Open-Data/Rentenbestandsstatistik/"
    "rentenbestandsstatistik-{year}.html"
)
RECORD_FORMAT = "bmas_rentenbestandsstatistik_csv"

# Most recent reporting year confirmed (2026-08-31) to have individual CSV
# downloads; 2025 currently only ships the combined Excel file (v1 doesn't
# ingest that -- see module docstring).
DEFAULT_YEARS = ["2024"]

# BMAS's actual encoding/delimiter (verified against a live download,
# see module docstring) -- both wrong per the response's own headers.
CSV_ENCODING = "latin-1"
CSV_DELIMITER = ";"


class BMASSource:
    """Lake source for BMAS's Rentenbestandsstatistik CSV downloads."""

    source_id = "bmas"

    def __init__(
        self,
        *,
        years: Optional[List[str]] = None,
        index_url_template: str = INDEX_URL_TEMPLATE,
        from_file: Optional[Path] = None,
        client: Optional[httpx.Client] = None,
        timeout: float = 30.0,
        user_agent: str = "study-scraper/0.0.1 (+https://github.com/cfischa/elt_data4transformation)",
    ) -> None:
        self._years = list(years or DEFAULT_YEARS)
        self._index_url_template = index_url_template
        self._from_file = from_file
        self._owns_client = client is None
        self._client = client or httpx.Client(
            timeout=timeout,
            headers={"User-Agent": user_agent},
            follow_redirects=True,
        )

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "BMASSource":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Public iterator
    # ------------------------------------------------------------------

    def iter_records(
        self,
        *,
        run_id: str,
        limit: Optional[int] = None,
    ) -> Iterator[SourceRecord]:
        if self._from_file is not None:
            yield from self._iter_from_file(run_id=run_id, limit=limit)
            return
        yield from self._iter_live(run_id=run_id, limit=limit)

    # ------------------------------------------------------------------
    # Live path: index page -> csv link discovery -> per-file fetch
    # ------------------------------------------------------------------

    def _iter_live(
        self,
        *,
        run_id: str,
        limit: Optional[int],
    ) -> Iterator[SourceRecord]:
        now = datetime.now(timezone.utc)
        yielded = 0
        for year in self._years:
            index_url = self._index_url_template.format(year=year)
            LOGGER.info("bmas GET %s", index_url)
            index_resp = get_with_retry(self._client, index_url)
            index_resp.raise_for_status()
            for filename, file_url in _extract_csv_links(
                index_resp.text, base_url=index_url
            ):
                LOGGER.info("bmas GET %s", file_url)
                file_resp = get_with_retry(self._client, file_url)
                file_resp.raise_for_status()
                record = self._build_record(
                    year=year,
                    filename=filename,
                    url=file_url,
                    content=file_resp.content,
                    index_url=index_url,
                    now=now,
                    run_id=run_id,
                )
                if record is None:
                    continue
                yield record
                yielded += 1
                if limit is not None and yielded >= limit:
                    return

    # ------------------------------------------------------------------
    # Fixture path
    # ------------------------------------------------------------------

    def _iter_from_file(
        self,
        *,
        run_id: str,
        limit: Optional[int],
    ) -> Iterator[SourceRecord]:
        assert self._from_file is not None
        raw = json.loads(self._from_file.read_text(encoding="utf-8"))
        now = datetime.now(timezone.utc)
        yielded = 0
        for entry in raw.get("files") or []:
            year = str(entry.get("year") or "").strip()
            filename = str(entry.get("filename") or "").strip()
            url = str(entry.get("url") or "").strip()
            csv_text = entry.get("csv_text")
            if not (year and filename and url and isinstance(csv_text, str)):
                continue
            record = self._build_record(
                year=year,
                filename=filename,
                url=url,
                content=csv_text.encode(CSV_ENCODING),
                index_url=entry.get("index_url"),
                now=now,
                run_id=run_id,
            )
            if record is None:
                continue
            yield record
            yielded += 1
            if limit is not None and yielded >= limit:
                return

    # ------------------------------------------------------------------
    # Record construction (shared by live + fixture)
    # ------------------------------------------------------------------

    def _build_record(
        self,
        *,
        year: str,
        filename: str,
        url: str,
        content: bytes,
        index_url: Optional[str],
        now: datetime,
        run_id: str,
    ) -> Optional[SourceRecord]:
        rows = _parse_csv_bytes(content)
        if not rows:
            return None
        canonical_url = _strip_query(url)
        content_hash = hashlib.sha256(content).hexdigest()
        payload: Dict[str, Any] = {
            "filename": filename,
            "year": year,
            "row_count": len(rows),
            "rows": rows,
        }
        return SourceRecord.build(
            source_id=self.source_id,
            source_record_id=f"{year}:{filename}",
            canonical_url=canonical_url,
            format=RECORD_FORMAT,
            content_type="text/csv",
            content_hash=content_hash,
            fetched_at=now,
            discovery_run_id=run_id,
            payload=payload,
            license=None,
            provenance={
                "fetch_source": "bmas_rentenbestandsstatistik_csv",
                "index_url": index_url,
                "download_url": url,
                "year": year,
                "filename": filename,
                "from_file": (
                    str(self._from_file) if self._from_file else None
                ),
            },
        )


def _extract_csv_links(html: str, *, base_url: str) -> List[Tuple[str, str]]:
    """Discover `.csv` download links on a BMAS year-index page.

    BMAS's download links aren't plain `<a href>` -- the site renders them
    as a custom `<pp-link href="...">` web component (verified live,
    2026-08-31), so this scans every tag with an `href` attribute rather
    than just `<a>`.

    Returns `(filename, absolute_url)` pairs, deduped and in document
    order.
    """
    soup = BeautifulSoup(html, "html.parser")
    links: List[Tuple[str, str]] = []
    seen: set = set()
    for tag in soup.find_all(href=True):
        href = tag["href"]
        if ".csv" not in href.lower():
            continue
        absolute = urljoin(base_url, href)
        if absolute in seen:
            continue
        seen.add(absolute)
        filename = urlsplit(absolute).path.rsplit("/", 1)[-1]
        links.append((filename, absolute))
    return links


def _strip_query(url: str) -> str:
    """Drop the `__blob`/`v` query params so `canonical_url` stays stable
    across BMAS's internal version bumps (`content_hash` still catches
    real content changes)."""
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def _parse_csv_bytes(content: bytes) -> List[List[str]]:
    text = content.decode(CSV_ENCODING)
    reader = csv.reader(io.StringIO(text), delimiter=CSV_DELIMITER)
    return list(reader)
