"""GESIS SSOAR discovery via OAI-PMH (Dublin Core).

Per DECISIONS.md A6, SSOAR is the first tier-1 source.

The source operates in two modes:

1. **Live** (default) — issues an OAI-PMH `ListRecords` request to
   https://www.ssoar.info/OAIHandler/request with metadataPrefix=oai_dc.
   Topic keywords are used as full-text filters where supported; SSOAR's
   OAI endpoint doesn't have native keyword filtering, so we paginate by
   resumption token and filter locally with the topic_filter module.

2. **Fixture** (`from_file=...`) — reads an OAI-PMH XML response from
   disk and parses it through the same code path. Used by tests and by
   the first dev-loop iteration (since the dev sandbox cannot reach
   ssoar.info; see docs/study_scraper/notes/first-run-2026-05-24.md).

Both modes share the same parser, so what tests verify is the
production path.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from xml.etree import ElementTree as ET

import httpx

from study_scraper.discovery.base import Candidate, DiscoverySource
from study_scraper.http import get_with_retry
from study_scraper.topics import Topic


LOGGER = logging.getLogger(__name__)

OAI_NS = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
    "dc": "http://purl.org/dc/elements/1.1/",
}

DEFAULT_BASE_URL = "https://www.ssoar.info/OAIHandler/request"

# ISO 639-2 -> 639-1 for the languages we care about.
_LANG_MAP = {
    "deu": "de",
    "ger": "de",
    "eng": "en",
    "fra": "fr",
    "fre": "fr",
}


class SSOARSource:
    """OAI-PMH discovery against GESIS SSOAR."""

    source_id = "ssoar"

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BASE_URL,
        from_file: Optional[Path] = None,
        client: Optional[httpx.Client] = None,
        timeout: float = 30.0,
        user_agent: str = "study-scraper/0.0.1 (+https://github.com/cfischa/elt_data4transformation)",
    ) -> None:
        self._base_url = base_url
        self._from_file = from_file
        self._owns_client = client is None
        self._client = client or httpx.Client(
            timeout=timeout,
            headers={"User-Agent": user_agent, "Accept": "application/xml"},
            follow_redirects=True,
        )

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "SSOARSource":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def iter_candidates(
        self, topic: Topic, *, limit: Optional[int] = None
    ) -> Iterator[Candidate]:
        """Yield candidates; live or from fixture file."""
        if self._from_file is not None:
            xml = self._from_file.read_text(encoding="utf-8")
            yield from self._parse_response(xml, topic=topic, limit=limit)
            return

        yielded = 0
        params: Dict[str, str] = {
            "verb": "ListRecords",
            "metadataPrefix": "oai_dc",
        }
        # SSOAR doesn't accept a `q=` keyword on OAI-PMH; we yield all
        # records in a date range and let topic_filter filter. The
        # caller passes --limit to keep the live run bounded.
        while True:
            LOGGER.info("SSOAR OAI-PMH request: %s", params)
            resp = get_with_retry(self._client, self._base_url, params=params)
            resp.raise_for_status()
            for cand in self._parse_response(resp.text, topic=topic, limit=None):
                yield cand
                yielded += 1
                if limit is not None and yielded >= limit:
                    return
            token = self._extract_resumption_token(resp.text)
            if not token:
                return
            params = {"verb": "ListRecords", "resumptionToken": token}

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_response(
        self,
        xml: str,
        *,
        topic: Topic,
        limit: Optional[int],
    ) -> Iterator[Candidate]:
        root = ET.fromstring(xml)
        records = root.findall(".//oai:record", OAI_NS)
        for i, rec in enumerate(records):
            if limit is not None and i >= limit:
                return
            cand = self._record_to_candidate(rec, topic=topic)
            if cand is not None:
                yield cand

    def _record_to_candidate(
        self, rec: ET.Element, *, topic: Topic
    ) -> Optional[Candidate]:
        # Skip deleted records.
        header = rec.find("oai:header", OAI_NS)
        if header is not None and header.get("status") == "deleted":
            return None

        identifier_el = (
            header.find("oai:identifier", OAI_NS) if header is not None else None
        )
        external_id = identifier_el.text.strip() if identifier_el is not None and identifier_el.text else ""

        dc = rec.find(".//oai_dc:dc", OAI_NS)
        if dc is None:
            return None

        titles = self._dc_texts(dc, "title")
        if not titles:
            return None
        title = titles[0]

        identifiers = self._dc_texts(dc, "identifier")
        canonical_url = next(
            (u for u in identifiers if u.startswith("http")),
            None,
        )
        if not canonical_url:
            return None

        # Some SSOAR records carry a DOI alongside the handle URL; pull
        # it out so the storage layer can dedup against OpenAlex.
        doi = next(
            (
                u
                for u in identifiers
                if "doi.org/" in u or u.lower().startswith("doi:")
                or u.startswith("10.")
            ),
            None,
        )

        authors = self._dc_texts(dc, "creator")
        publishers = self._dc_texts(dc, "publisher")
        publisher = publishers[0] if publishers else None
        abstract = "\n".join(self._dc_texts(dc, "description")) or None
        publication_date = _parse_date(self._dc_texts(dc, "date"))
        language = _normalize_language(self._dc_texts(dc, "language"))

        return Candidate(
            source_id=self.source_id,
            external_id=external_id or canonical_url,
            canonical_url=canonical_url,
            title=title,
            authors=authors,
            publisher=publisher,
            publication_date=publication_date,
            language=language,
            abstract=abstract,
            doi=doi,
            discovery_query=topic.id,
            raw={
                "subjects": self._dc_texts(dc, "subject"),
                "types": self._dc_texts(dc, "type"),
                "rights": self._dc_texts(dc, "rights"),
            },
        )

    @staticmethod
    def _dc_texts(dc: ET.Element, tag: str) -> List[str]:
        out: List[str] = []
        for el in dc.findall(f"dc:{tag}", OAI_NS):
            if el.text:
                text = el.text.strip()
                if text:
                    out.append(text)
        return out

    @staticmethod
    def _extract_resumption_token(xml: str) -> Optional[str]:
        try:
            root = ET.fromstring(xml)
        except ET.ParseError:
            return None
        el = root.find(".//oai:resumptionToken", OAI_NS)
        if el is None or not el.text:
            return None
        return el.text.strip() or None


def _parse_date(values: List[str]) -> Optional[date]:
    for value in values:
        for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
    return None


_KNOWN_ISO_639_1 = {"de", "en", "fr", "es", "it", "nl", "pl", "pt", "ru"}


def _normalize_language(values: List[str]) -> Optional[str]:
    for value in values:
        key = value.strip().lower()
        if key in _LANG_MAP:
            return _LANG_MAP[key]
        if key in _KNOWN_ISO_639_1:
            return key
    return None
