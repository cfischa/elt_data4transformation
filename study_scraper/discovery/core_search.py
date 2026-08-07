"""CORE (core.ac.uk) discovery via the public `/search/works` REST API.

Issue #88 originally scoped BASE as the third catalog-style source
feeding `studies` (GOAL.md's ≥3-sources topic-coverage bar; only
`openalex`+`ssoar` count today). BASE turned out to be access-key/
IP-allowlist gated (DECISIONS.md A35) and DOAJ's `robots.txt`
specifically disallows `ClaudeBot` (A37). CORE was scouted as the next
candidate: `api.core.ac.uk/v3/search/works/` answers real, relevance-
ranked queries with no API key at all (returns `200` with structured
JSON — title, abstract, authors, DOI, publication date, download URL;
full text is withheld for keyless callers, which we don't need), and
neither `core.ac.uk/robots.txt` nor `api.core.ac.uk/robots.txt` carries
a `Disallow` line (unlike DOAJ's explicit `ClaudeBot` exclusion). See
DECISIONS.md for the full write-up.

Two modes share one parser (house pattern, mirrors openalex.py):

1. **Live** (default) — `httpx.Client` GET against
   `https://api.core.ac.uk/v3/search/works/` with an OR-joined `q` built
   from the topic's include keywords + synonyms, `offset`-paginated.
   CORE's relevance ranking (not a strict boolean filter) surfaces
   on-topic works first even though the query itself matches broadly —
   the same shape as OpenAlex's `search=` parameter. Local stage-1
   topic filtering (`topic_filter.score_text`) still does the real
   keep/drop decision downstream in `pipeline.py`.

2. **Fixture** (`from_file=...`) — reads a saved CORE JSON response
   from disk and parses it through the same path. Used by tests and any
   environment without outbound network access.

The anonymous tier is rate-limited (observed `x-ratelimit-limit: 10`);
`http.get_with_retry` already honours `Retry-After` on 429s, so no
extra handling is needed here.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime
from itertools import zip_longest
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional

import httpx

from study_scraper.discovery.base import Candidate
from study_scraper.http import get_with_retry, polite_sleep
from study_scraper.topics import Topic

LOGGER = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.core.ac.uk/v3/search/works/"
DEFAULT_LIMIT = 25
# Same rationale as OpenAlex's _MAX_SEARCH_TERMS: enough OR-joined quoted
# terms to cover a topic's core vocabulary without an unwieldy query string.
_MAX_SEARCH_TERMS = 24


class CoreSearchSource:
    """Discovery against CORE `/search/works`."""

    source_id = "core"

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BASE_URL,
        from_file: Optional[Path] = None,
        client: Optional[httpx.Client] = None,
        timeout: float = 30.0,
        user_agent: str = "study-scraper/0.0.1 (+https://github.com/cfischa/elt_data4transformation)",
        limit: int = DEFAULT_LIMIT,
        politeness_delay: float = 0.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._base_url = base_url
        self._from_file = from_file
        self._limit = limit
        self._politeness_delay = politeness_delay
        self._sleep = sleep
        self._owns_client = client is None
        self._client = client or httpx.Client(
            timeout=timeout,
            headers={"User-Agent": user_agent, "Accept": "application/json"},
            follow_redirects=True,
        )

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "CoreSearchSource":
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
            yielded = 0
            for cand in self._parse_payload(payload, topic=topic):
                yield cand
                yielded += 1
                if limit is not None and yielded >= limit:
                    return
            return

        query = _build_search_query(topic)
        yielded = 0
        offset = 0
        page = 0
        while True:
            if page > 0:
                polite_sleep(self._politeness_delay, sleep=self._sleep)
            page += 1
            params: Dict[str, str] = {
                "q": query,
                "limit": str(self._limit),
                "offset": str(offset),
            }
            LOGGER.info("CORE request: q=%r offset=%d", query, offset)
            resp = get_with_retry(self._client, self._base_url, params=params)
            resp.raise_for_status()
            payload = resp.json()
            results = payload.get("results") or []
            if not results:
                return
            for cand in self._parse_payload(payload, topic=topic):
                yield cand
                yielded += 1
                if limit is not None and yielded >= limit:
                    return
            offset += len(results)
            total_hits = payload.get("totalHits")
            if isinstance(total_hits, int) and offset >= total_hits:
                return

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_payload(
        self, payload: Dict[str, Any], *, topic: Topic
    ) -> Iterator[Candidate]:
        for work in payload.get("results") or []:
            cand = self._work_to_candidate(work, topic=topic)
            if cand is not None:
                yield cand

    def _work_to_candidate(
        self, work: Dict[str, Any], *, topic: Topic
    ) -> Optional[Candidate]:
        title = (work.get("title") or "").strip()
        if not title:
            return None

        core_id = str(work.get("id") or "").strip()
        doi = (work.get("doi") or "").strip() if work.get("doi") else ""
        download_url = (work.get("downloadUrl") or "").strip()
        display_url = _display_url(work)
        # CORE returns bare DOIs ("10.xxx/yyy"), unlike OpenAlex which
        # already prefixes them as `https://doi.org/...`; build the same
        # navigable URL form here so canonical_url stays a fetchable link
        # (pipeline._normalize_doi strips the prefix back off for storage).
        doi_url = f"https://doi.org/{doi}" if doi else ""
        # DOI first (matches other sources' cross-source dedup key),
        # then the download link, then CORE's own work page.
        canonical_url = doi_url or download_url or display_url
        if not canonical_url:
            return None

        authors = [
            (a.get("name") or "").strip()
            for a in (work.get("authors") or [])
            if isinstance(a, dict) and (a.get("name") or "").strip()
        ]

        language_obj = work.get("language") or {}
        language = (
            (language_obj.get("code") or "").strip().lower() or None
            if isinstance(language_obj, dict)
            else None
        )

        pub_date = _parse_published_date(work.get("publishedDate")) or _year_to_date(
            work.get("yearPublished")
        )

        data_providers = [
            dp.get("name")
            for dp in (work.get("dataProviders") or [])[:5]
            if isinstance(dp, dict) and dp.get("name")
        ]

        return Candidate(
            source_id=self.source_id,
            external_id=core_id or doi or canonical_url,
            canonical_url=canonical_url,
            title=title,
            authors=authors,
            publisher=(work.get("publisher") or "").strip() or None,
            publication_date=pub_date,
            language=language,
            abstract=(work.get("abstract") or "").strip() or None,
            doi=doi or None,
            discovery_query=topic.id,
            raw={
                "core_id": core_id or None,
                "download_url": download_url or None,
                "document_type": (work.get("documentType") or "").strip() or None,
                "data_providers": data_providers,
            },
        )


# ----------------------------------------------------------------------
# Helpers (also used by tests)
# ----------------------------------------------------------------------


def _display_url(work: Dict[str, Any]) -> Optional[str]:
    for link in work.get("links") or []:
        if isinstance(link, dict) and link.get("type") == "display" and link.get("url"):
            return link["url"]
    core_id = work.get("id")
    return f"https://core.ac.uk/works/{core_id}" if core_id else None


def _build_search_query(topic: Topic) -> str:
    """OR-join topic terms into CORE's `q` parameter.

    Mirrors openalex.py's `_build_search_query`: CORE's default query
    behaviour is a relevance-ranked match across the whole corpus, not a
    strict boolean AND, so terms must be OR-joined (an AND/space-joined
    list would demand every term appear at once and starve recall the
    same way issue #26 did for OpenAlex). Terms are taken round-robin
    across locales so a topic with many German synonyms doesn't crowd
    out its English include_keywords before the cap.
    """
    per_locale: List[List[str]] = [
        loc.include_keywords + loc.synonyms for loc in topic.locales.values()
    ]
    terms: List[str] = []
    seen: set[str] = set()
    for tier in zip_longest(*per_locale):
        for term in tier:
            if term is None:
                continue
            key = term.strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            cleaned = term.strip()
            if " " in cleaned:
                cleaned = f'"{cleaned}"'
            terms.append(cleaned)
    return " OR ".join(terms[:_MAX_SEARCH_TERMS])


def _parse_published_date(value: Any) -> Optional[date]:
    if not value or not isinstance(value, str):
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(value, fmt)
            return parsed.date()
        except ValueError:
            continue
    return None


def _year_to_date(value: Any) -> Optional[date]:
    if isinstance(value, int) and 1900 < value < 2100:
        return date(value, 1, 1)
    return None
