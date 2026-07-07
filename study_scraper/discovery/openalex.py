"""OpenAlex discovery via the public `/works` REST API.

Per DECISIONS.md A6, OpenAlex is the second tier-1 source. Compared to
SSOAR, OpenAlex covers a much larger universe of academic works
(including German policy/social-science publications) and exposes them
through a structured JSON API with explicit language and date filters.

Two modes share the same parser (mirrors the SSOAR pattern):

1. **Live** (default) — `httpx.Client` GET against
   `https://api.openalex.org/works` with `search`, `filter` and `per-page`.
   Topic include keywords are OR-joined into the `search` parameter; the
   `language` filter is applied per locale.

2. **Fixture** (`from_file=...`) — reads a saved OpenAlex JSON response
   from disk and parses it through the same path. Used by tests and by
   any environment without outbound network access.

Abstracts in OpenAlex are inverted indexes (word → positions) to avoid
redistributing copyrighted text; we reconstruct a plain string before
handing the candidate to the topic filter.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import httpx

from study_scraper.discovery.base import Candidate, DiscoverySource
from study_scraper.http import get_with_retry
from study_scraper.topics import Topic


LOGGER = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.openalex.org/works"
DEFAULT_PER_PAGE = 25
MAX_PER_PAGE = 200


class OpenAlexSource:
    """Discovery against OpenAlex `/works`."""

    source_id = "openalex"

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BASE_URL,
        from_file: Optional[Path] = None,
        client: Optional[httpx.Client] = None,
        mailto: Optional[str] = None,
        timeout: float = 30.0,
        user_agent: str = "study-scraper/0.0.1 (+https://github.com/cfischa/elt_data4transformation)",
        per_page: int = DEFAULT_PER_PAGE,
        work_ids: Optional[List[str]] = None,
    ) -> None:
        self._base_url = base_url
        self._from_file = from_file
        self._mailto = mailto  # Joining the "polite pool" gets better rate limits
        self._per_page = min(per_page, MAX_PER_PAGE)
        # Reference-follower mode (Phase 5d): fetch these specific works
        # instead of running a keyword search. IDs are openalex.org URLs
        # or bare W-ids; up to ~50 per request (OpenAlex OR-filter cap).
        self._work_ids = [
            w.rsplit("/", 1)[-1] for w in (work_ids or []) if w
        ]
        self._owns_client = client is None
        self._client = client or httpx.Client(
            timeout=timeout,
            headers={"User-Agent": user_agent, "Accept": "application/json"},
            follow_redirects=True,
        )

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "OpenAlexSource":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Iteration
    # ------------------------------------------------------------------

    def iter_candidates(
        self, topic: Topic, *, limit: Optional[int] = None
    ) -> Iterator[Candidate]:
        if self._from_file is not None:
            payload = json.loads(self._from_file.read_text(encoding="utf-8"))
            yield from self._parse_payload(payload, topic=topic, limit=limit)
            return

        params: Dict[str, str] = {"per-page": str(self._per_page)}
        if self._work_ids:
            # Reference-follower mode: fetch exactly these works.
            # NOTE for first live run: verify the filter attribute —
            # `ids.openalex:` per OpenAlex docs; fall back to
            # `openalex:` if the API rejects it.
            params["filter"] = "ids.openalex:" + "|".join(self._work_ids)
        else:
            search_query = _build_search_query(topic)
            languages = sorted({loc for loc in topic.locales} & {"de", "en"})
            params["search"] = search_query
            if languages:
                params["filter"] = "language:" + "|".join(languages)
        if self._mailto:
            params["mailto"] = self._mailto

        yielded = 0
        cursor = "*"
        while True:
            params["cursor"] = cursor
            LOGGER.info("OpenAlex request: %s", params)
            resp = get_with_retry(self._client, self._base_url, params=params)
            resp.raise_for_status()
            payload = resp.json()
            for cand in self._parse_payload(payload, topic=topic, limit=None):
                yield cand
                yielded += 1
                if limit is not None and yielded >= limit:
                    return
            meta = payload.get("meta") or {}
            next_cursor = meta.get("next_cursor")
            if not next_cursor or next_cursor == cursor:
                return
            cursor = next_cursor

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_payload(
        self,
        payload: Dict[str, Any],
        *,
        topic: Topic,
        limit: Optional[int],
    ) -> Iterator[Candidate]:
        results = payload.get("results") or []
        for i, work in enumerate(results):
            if limit is not None and i >= limit:
                return
            cand = self._work_to_candidate(work, topic=topic)
            if cand is not None:
                yield cand

    def _work_to_candidate(
        self, work: Dict[str, Any], *, topic: Topic
    ) -> Optional[Candidate]:
        title = (work.get("title") or work.get("display_name") or "").strip()
        if not title:
            return None

        openalex_id = (work.get("id") or "").strip()
        doi = (work.get("doi") or "").strip()
        # canonical_url stays preferring DOI -> openalex_id for backwards
        # compat with already-ingested rows; `doi` is also surfaced as
        # its own Candidate field so the storage layer can dedup across
        # sources that don't share canonical URLs.
        canonical_url = doi or openalex_id
        if not canonical_url:
            return None

        external_id = openalex_id or doi

        authors: List[str] = []
        for authorship in work.get("authorships") or []:
            author = (authorship or {}).get("author") or {}
            name = (author.get("display_name") or "").strip()
            if name:
                authors.append(name)

        publisher: Optional[str] = None
        primary_location = work.get("primary_location") or {}
        source = primary_location.get("source") or {}
        if source.get("display_name"):
            publisher = source["display_name"]

        pub_date = _parse_iso_date(work.get("publication_date")) or _year_to_date(
            work.get("publication_year")
        )
        language = (work.get("language") or "").lower() or None
        abstract = _reconstruct_abstract(work.get("abstract_inverted_index"))

        # Pull a small structured "raw" payload — enough for downstream
        # debugging without dragging the full 50-field document along.
        topics_raw: List[Dict[str, Any]] = []
        for t in (work.get("topics") or [])[:5]:
            if isinstance(t, dict):
                topics_raw.append(
                    {
                        "display_name": t.get("display_name"),
                        "score": t.get("score"),
                        "subfield": (t.get("subfield") or {}).get("display_name"),
                    }
                )
        keywords_raw: List[str] = []
        for kw in (work.get("keywords") or [])[:10]:
            if isinstance(kw, dict) and kw.get("display_name"):
                keywords_raw.append(kw["display_name"])

        # Phase 5d step 1: capture citation-graph IDs. The actual
        # reference-follower (which fetches these IDs as new candidates)
        # lands later; for now we just preserve them in `raw` so the
        # operator can query them and the follower has a foundation.
        referenced_works: List[str] = [
            r for r in (work.get("referenced_works") or []) if isinstance(r, str)
        ]
        related_works: List[str] = [
            r for r in (work.get("related_works") or []) if isinstance(r, str)
        ]

        return Candidate(
            source_id=self.source_id,
            external_id=external_id or canonical_url,
            canonical_url=canonical_url,
            title=title,
            authors=authors,
            publisher=publisher,
            publication_date=pub_date,
            language=language,
            abstract=abstract,
            doi=doi or None,
            discovery_query=topic.id,
            raw={
                "openalex_id": openalex_id,
                "doi": doi or None,
                # Fetchable document locations — openalex.org work pages
                # are bot-protected metadata, so fulltext needs these.
                "landing_page_url": primary_location.get("landing_page_url"),
                "pdf_url": primary_location.get("pdf_url"),
                "type": work.get("type"),
                "topics": topics_raw,
                "keywords": keywords_raw,
                # Phase 5d foundation -- citation graph for the future
                # reference-follower. Cap at 200 per side so we don't
                # bloat the row when a paper cites a long bibliography.
                "referenced_works": referenced_works[:200],
                "related_works": related_works[:200],
            },
        )


# ----------------------------------------------------------------------
# Helpers (also used by tests)
# ----------------------------------------------------------------------


def _build_search_query(topic: Topic) -> str:
    """Build a `search` parameter from the topic's include + synonym terms.

    OpenAlex combines space-separated search terms with **AND** — a
    space-joined keyword list demands a document match ALL terms at once,
    which returned seen=0 on live runs (issue #26, observed 2026-07-02).
    Terms must be OR-joined explicitly; multi-word terms are quoted so
    their words stay one phrase.
    """
    terms: List[str] = []
    seen: set[str] = set()
    for locale in topic.locales.values():
        for term in locale.include_keywords + locale.synonyms:
            key = term.strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            cleaned = term.strip()
            if " " in cleaned:
                cleaned = f'"{cleaned}"'
            terms.append(cleaned)
    # Cap to keep URL length sane — most relevant signal is in the first
    # half-dozen terms anyway.
    return " OR ".join(terms[:8])


def _reconstruct_abstract(idx: Optional[Dict[str, List[int]]]) -> Optional[str]:
    if not idx:
        return None
    positions: List[tuple[int, str]] = []
    for word, locs in idx.items():
        for loc in locs:
            if isinstance(loc, int):
                positions.append((loc, word))
    if not positions:
        return None
    positions.sort()
    return " ".join(word for _, word in positions)


def _parse_iso_date(value: Any) -> Optional[date]:
    if not value or not isinstance(value, str):
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _year_to_date(value: Any) -> Optional[date]:
    if isinstance(value, int) and 1900 < value < 2100:
        return date(value, 1, 1)
    return None
