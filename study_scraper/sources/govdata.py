"""GovData.de (CKAN) lake source (A14 / issue #64).

GovData (govdata.de) is Germany's cross-government open-data catalog --
a CKAN instance aggregating dataset metadata from federal, state, and
municipal agencies (BMAS, UBA, BAMF, Destatis, city/state portals, ...)
under one catalog. It's our first pure-government lake source
(`bundestag_dip` is discovery-only and currently broken, see #48), and
fills the "government" slot in GOAL.md's source-coverage bar.

API: CKAN's `package_search` action, free and unauthenticated:

    https://www.govdata.de/ckan/api/3/action/package_search?q=<term>

Modes (mirrors DAWUM / Eurostat / GESIS / Eurobarometer):

  * `from_file=<path>` -- reads one saved `package_search` JSON response
    from disk (the real CKAN response shape: `{"result": {"count": ...,
    "results": [...]}}`). Used by tests and no-network environments.
  * Live (default) -- runs `package_search` once per query term,
    offset-paginated, deduped by dataset id across terms.

License: CKAN's default is "Data License Germany 2.0" (`dl-de/by-2-0`),
but per-dataset overrides exist, so every record captures its own
`license_title`/`license_id` rather than a hardcoded blanket license.
Per A14 the full dataset payload (incl. `resources` download URLs) is
preserved as-is; per-dataset typed projections are a later SQL view.

Filtering (non-DE hits, irrelevant resource types) is deliberately out
of scope for v1 -- coverage-first, same as A12/A14.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import httpx

from study_scraper.http import get_with_retry
from study_scraper.models import SourceRecord


LOGGER = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://www.govdata.de/ckan/api/3/action/package_search"
GOVDATA_DEFAULT_LICENSE = "Data License Germany 2.0 (dl-de/by-2-0)"
RECORD_FORMAT = "govdata_ckan_package"

# How many topic include_keywords to query per run (one request chain
# each) -- bounds request volume the same way bundestag_dip._MAX_TERMS
# does.
DEFAULT_MAX_TERMS = 6

# CKAN package_search page size.
DEFAULT_ROWS = 100


class GovDataSource:
    """Lake source for the GovData.de CKAN `package_search` API."""

    source_id = "govdata"

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BASE_URL,
        from_file: Optional[Path] = None,
        queries: Optional[List[str]] = None,
        rows: int = DEFAULT_ROWS,
        client: Optional[httpx.Client] = None,
        timeout: float = 30.0,
        user_agent: str = "study-scraper/0.0.1 (+https://github.com/cfischa/elt_data4transformation)",
    ) -> None:
        self._base_url = base_url
        self._from_file = from_file
        self._queries = list(queries or [])
        self._rows = rows
        self._owns_client = client is None
        self._client = client or httpx.Client(
            timeout=timeout,
            headers={"User-Agent": user_agent, "Accept": "application/json"},
            follow_redirects=True,
        )

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "GovDataSource":
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

        if not self._queries:
            raise RuntimeError(
                "GovDataSource live mode requires queries=[...] (search "
                "terms, e.g. a topic's include_keywords) or "
                "from_file=<path>."
            )
        yield from self._iter_live(run_id=run_id, limit=limit)

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
        results = ((raw.get("result") or {}).get("results")) or []
        now = datetime.now(timezone.utc)
        yielded = 0
        for dataset in results:
            record = self._build_record(dataset=dataset, now=now, run_id=run_id)
            if record is None:
                continue
            yield record
            yielded += 1
            if limit is not None and yielded >= limit:
                return

    # ------------------------------------------------------------------
    # Live path
    # ------------------------------------------------------------------

    def _iter_live(
        self,
        *,
        run_id: str,
        limit: Optional[int],
    ) -> Iterator[SourceRecord]:
        now = datetime.now(timezone.utc)
        yielded = 0
        seen_ids: set = set()
        for term in self._queries:
            start = 0
            while True:
                payload = self._fetch_page(term=term, start=start)
                result = payload.get("result") or {}
                datasets = result.get("results") or []
                if not datasets:
                    break
                for dataset in datasets:
                    dataset_id = str(dataset.get("id") or dataset.get("name") or "")
                    if not dataset_id or dataset_id in seen_ids:
                        continue
                    seen_ids.add(dataset_id)
                    record = self._build_record(
                        dataset=dataset, now=now, run_id=run_id, query=term,
                    )
                    if record is None:
                        continue
                    yield record
                    yielded += 1
                    if limit is not None and yielded >= limit:
                        return
                start += len(datasets)
                if start >= int(result.get("count") or 0):
                    break

    def _fetch_page(self, *, term: str, start: int) -> Dict[str, Any]:
        params = {"q": term, "rows": self._rows, "start": start}
        LOGGER.info("govdata GET %s params=%s", self._base_url, params)
        resp = get_with_retry(self._client, self._base_url, params=params)
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Record construction (shared by live + fixture)
    # ------------------------------------------------------------------

    def _build_record(
        self,
        *,
        dataset: Dict[str, Any],
        now: datetime,
        run_id: str,
        query: Optional[str] = None,
    ) -> Optional[SourceRecord]:
        dataset_id = str(dataset.get("id") or dataset.get("name") or "").strip()
        name = str(dataset.get("name") or "").strip()
        if not dataset_id:
            return None

        canonical_url = f"https://www.govdata.de/dataset/{name or dataset_id}"
        license_ = (
            dataset.get("license_title")
            or dataset.get("license_id")
            or GOVDATA_DEFAULT_LICENSE
        )

        payload_bytes = json.dumps(
            dataset, ensure_ascii=False, sort_keys=True
        ).encode("utf-8")
        content_hash = hashlib.sha256(payload_bytes).hexdigest()

        organization = dataset.get("organization") or {}

        return SourceRecord.build(
            source_id=self.source_id,
            source_record_id=dataset_id,
            canonical_url=canonical_url,
            format=RECORD_FORMAT,
            content_type="application/json",
            content_hash=content_hash,
            fetched_at=now,
            discovery_run_id=run_id,
            payload=dataset,
            license=license_,
            provenance={
                "fetch_source": "govdata_ckan_package_search",
                "endpoint": self._base_url,
                "query": query,
                "from_file": (
                    str(self._from_file) if self._from_file else None
                ),
                "title_hint": dataset.get("title"),
                "publisher_hint": organization.get("title") or organization.get("name"),
            },
        )


def topic_query_terms(
    topic: Any, *, max_terms: int = DEFAULT_MAX_TERMS
) -> List[str]:
    """Derive `package_search` terms from a topic's `include_keywords`.

    Mirrors `bundestag_dip._search_terms` (per-topic keyword queries),
    but uses only `include_keywords` -- not synonyms -- per #64's ask:
    include_keywords are the specific terms (e.g. "Erbschaftssteuer",
    not the umbrella "Steuern"), which keep government-dataset search
    results relevant. German locale first (GovData is a German portal).
    """
    terms: List[str] = []
    seen: set = set()
    locales = sorted(
        topic.locales.items(), key=lambda kv: kv[0] != "de"
    )
    for _lang, locale in locales:
        for term in locale.include_keywords:
            key = term.strip().lower()
            if key and key not in seen:
                seen.add(key)
                terms.append(term.strip())
    return terms[:max_terms]
