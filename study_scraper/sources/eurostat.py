"""Eurostat dissemination API source (lake-style, A14).

Eurostat publishes statistical tables via a free, no-auth REST API
returning JSON-stat 2.0:

    https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/
    data/{datasetCode}?format=json&lang=en[&filter=value]

Unlike DAWUM (one big root document) and GESIS (catalog of every
schema:Dataset), Eurostat requires the operator to pick specific
dataset codes (e.g. `env_air_gge` for greenhouse-gas emissions,
`nrg_cb_e` for energy balances). The source therefore takes one or
more codes per ingest run.

Per A14 we preserve the raw JSON-stat payload as-is. Per-table typed
projections happen later via SQL views (e.g. `eurostat_ghg` would
explode the env_air_gge payload into typed rows).

License: Eurostat re-use policy (CC BY 4.0 with attribution). Captured
on every emitted SourceRecord.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import httpx

from study_scraper.models import SourceRecord


LOGGER = logging.getLogger(__name__)

DEFAULT_BASE_URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
)
EUROSTAT_LICENSE = "Eurostat re-use policy (CC BY 4.0)"
RECORD_FORMAT = "eurostat_jsonstat"
VIEWER_URL = "https://ec.europa.eu/eurostat/databrowser/view/{code}"


class EurostatSource:
    """Lake source for the Eurostat dissemination API."""

    source_id = "eurostat"

    def __init__(
        self,
        *,
        codes: Optional[List[str]] = None,
        base_url: str = DEFAULT_BASE_URL,
        from_file: Optional[Path] = None,
        client: Optional[httpx.Client] = None,
        timeout: float = 60.0,
        user_agent: str = "study-scraper/0.0.1 (+https://github.com/cfischa/elt_data4transformation)",
        lang: str = "en",
    ) -> None:
        # In `from_file` mode the dataset code is taken from each
        # record in the file (see _iter_from_file). Live mode requires
        # at least one explicit code.
        self._codes = list(codes or [])
        self._base_url = base_url.rstrip("/")
        self._from_file = from_file
        self._lang = lang
        self._owns_client = client is None
        self._client = client or httpx.Client(
            timeout=timeout,
            headers={"User-Agent": user_agent, "Accept": "application/json"},
            follow_redirects=True,
        )

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "EurostatSource":
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

        if not self._codes:
            raise RuntimeError(
                "EurostatSource live mode requires at least one dataset "
                "code; pass codes=[...] or use from_file=<path>."
            )
        now = datetime.now(timezone.utc)
        yielded = 0
        for code in self._codes:
            url = f"{self._base_url}/{code}?format=json&lang={self._lang}"
            LOGGER.info("eurostat GET %s", url)
            resp = self._client.get(url)
            resp.raise_for_status()
            payload = resp.json()
            record = self._build_record(
                code=code, payload=payload, now=now, run_id=run_id,
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
        # Two acceptable fixture shapes:
        #   (a) a raw JSON-stat document (single dataset)
        #   (b) {"datasets": [{"code": "env_air_gge", "payload": {...}},
        #                      ...]}  -- multiple datasets in one file
        # If the document looks like JSON-stat (class==dataset), wrap.
        raw = json.loads(self._from_file.read_text(encoding="utf-8"))
        now = datetime.now(timezone.utc)
        yielded = 0
        entries: List[Dict[str, Any]]
        if isinstance(raw, dict) and raw.get("class") == "dataset":
            # Single dataset fixture; derive code from `extension.id`
            # if Eurostat included it, else demand caller supplied codes.
            code = (
                (raw.get("extension") or {}).get("id")
                or (raw.get("href") or "").rsplit("/", 1)[-1].split("?", 1)[0]
                or (self._codes[0] if self._codes else None)
            )
            if not code:
                raise ValueError(
                    "single-dataset Eurostat fixture must include "
                    "extension.id, an href, or be paired with codes=[...]"
                )
            entries = [{"code": code, "payload": raw}]
        else:
            entries = list(raw.get("datasets") or [])

        for entry in entries:
            code = (entry.get("code") or "").strip()
            payload = entry.get("payload")
            if not code or not isinstance(payload, dict):
                continue
            record = self._build_record(
                code=code, payload=payload, now=now, run_id=run_id,
            )
            if record is None:
                continue
            yield record
            yielded += 1
            if limit is not None and yielded >= limit:
                return

    # ------------------------------------------------------------------
    # Record construction
    # ------------------------------------------------------------------

    def _build_record(
        self,
        *,
        code: str,
        payload: Dict[str, Any],
        now: datetime,
        run_id: str,
    ) -> Optional[SourceRecord]:
        if not payload:
            return None
        canonical_url = VIEWER_URL.format(code=code)
        title_hint = payload.get("label") or code
        updated = payload.get("updated")
        size = payload.get("size") or []
        # Hash the canonical sorted-keys payload so re-fetching the
        # same data hashes the same way.
        payload_bytes = json.dumps(
            payload, ensure_ascii=False, sort_keys=True
        ).encode("utf-8")
        content_hash = hashlib.sha256(payload_bytes).hexdigest()

        return SourceRecord.build(
            source_id=self.source_id,
            source_record_id=code,
            canonical_url=canonical_url,
            format=RECORD_FORMAT,
            content_type="application/json",
            content_hash=content_hash,
            fetched_at=now,
            discovery_run_id=run_id,
            payload=payload,
            license=EUROSTAT_LICENSE,
            provenance={
                "fetch_source": "eurostat_dissemination_v1",
                "base_url": self._base_url,
                "from_file": (
                    str(self._from_file) if self._from_file else None
                ),
                "code": code,
                "title_hint": title_hint,
                "updated": updated,
                "size": size,
            },
        )
