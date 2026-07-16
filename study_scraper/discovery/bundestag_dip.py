"""Bundestag DIP discovery — parliamentary documents per topic.

ROADMAP source #6 and one half of the flagship democratic use case
(product-expansion Axis 4): the opinion–policy gap view joins majority
opinion per question cluster with what parliament actually debates and
decides. This source ingests the parliament half: Drucksachen
(Anträge, Gesetzentwürfe, Antworten) matched to topic keywords, as
catalog-style candidates flowing through the normal topic filter.

API: DIP (Dokumentations- und Informationssystem für Parlamentsmaterialien),
`https://search.dip.bundestag.de/api/v1` — structured JSON REST.

Auth: an API key sent as the `apikey` query parameter. The Bundestag
PUBLISHES a shared public key in the API documentation
(https://dip.bundestag.de/über-dip/hilfe/api) and rotates it every year
or two; a personal key is free by mail to infoline.id3@bundestag.de.
Resolution order here: explicit `api_key=` arg → `DIP_API_KEY` env var
→ the published public key below (may age out — then set DIP_API_KEY).

Two modes share one parser (house pattern):
  1. Live — GET /drucksache with `f.titel` per topic keyword,
     cursor-paginated.
  2. Fixture (`from_file=`) — a saved JSON response; test path and
     no-network environments.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime
from itertools import zip_longest
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import httpx

from study_scraper.discovery.base import Candidate
from study_scraper.http import get_with_retry
from study_scraper.topics import Topic

LOGGER = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://search.dip.bundestag.de/api/v1"

# The documented shared key (DIP help page, valid into 2026 per the
# docs at time of writing). Rotates: if live calls return 401, fetch
# the current one from the help page or request a personal key.
PUBLIC_API_KEY = "OSOegLs.PR2lwJ1dwCeje9vTj7FPOt3hvpYKtwKkhw"

# How many topic keywords to query per run (one request chain each).
# 8, not 4: at 4 the umbrella synonyms filled the whole budget and the
# specific include_keywords (Wehrpflicht, Mietpreisbremse, Rentenein-
# trittsalter — the terms the question registry depends on) were dead
# config for every topic (audit 2026-07-11).
_MAX_TERMS = 8


def resolve_api_key(explicit: Optional[str] = None) -> str:
    return explicit or os.environ.get("DIP_API_KEY") or PUBLIC_API_KEY


class BundestagDIPSource:
    """Discovery against DIP `/drucksache`."""

    source_id = "bundestag_dip"

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BASE_URL,
        from_file: Optional[Path] = None,
        client: Optional[httpx.Client] = None,
        api_key: Optional[str] = None,
        timeout: float = 30.0,
        user_agent: str = "study-scraper/0.0.1 (+https://github.com/cfischa/elt_data4transformation)",
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._from_file = from_file
        self._api_key = resolve_api_key(api_key)
        self._owns_client = client is None
        self._client = client or httpx.Client(
            timeout=timeout,
            headers={"User-Agent": user_agent, "Accept": "application/json"},
            follow_redirects=True,
        )

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "BundestagDIPSource":
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

        yielded = 0
        seen_ids: set = set()
        for term in _search_terms(topic):
            cursor: Optional[str] = None
            while True:
                params: Dict[str, str] = {
                    "apikey": self._api_key,
                    "f.titel": term,
                    "format": "json",
                }
                if cursor:
                    params["cursor"] = cursor
                LOGGER.info("DIP request: f.titel=%r cursor=%s", term, cursor)
                resp = get_with_retry(
                    self._client, f"{self._base_url}/drucksache", params=params
                )
                resp.raise_for_status()
                payload = resp.json()
                got_any = False
                for cand in self._parse_payload(payload, topic=topic):
                    got_any = True
                    if cand.external_id in seen_ids:
                        continue
                    seen_ids.add(cand.external_id)
                    yield cand
                    yielded += 1
                    if limit is not None and yielded >= limit:
                        return
                next_cursor = payload.get("cursor")
                # DIP signals the last page by repeating the cursor.
                if not got_any or not next_cursor or next_cursor == cursor:
                    break
                cursor = next_cursor

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_payload(
        self, payload: Dict[str, Any], *, topic: Topic
    ) -> Iterator[Candidate]:
        for doc in payload.get("documents") or []:
            cand = self._doc_to_candidate(doc, topic=topic)
            if cand is not None:
                yield cand

    def _doc_to_candidate(
        self, doc: Dict[str, Any], *, topic: Topic
    ) -> Optional[Candidate]:
        title = (doc.get("titel") or "").strip()
        doc_id = str(doc.get("id") or "").strip()
        if not title or not doc_id:
            return None

        fundstelle = doc.get("fundstelle") or {}
        pdf_url = (fundstelle.get("pdf_url") or "").strip()
        canonical_url = pdf_url or f"https://dip.bundestag.de/drucksache/{doc_id}"

        urheber = [
            u.get("titel") or u.get("bezeichnung") or ""
            for u in (doc.get("urheber") or [])
            if isinstance(u, dict)
        ]
        publisher = "; ".join(filter(None, urheber)) or "Deutscher Bundestag"

        return Candidate(
            source_id=self.source_id,
            external_id=doc_id,
            canonical_url=canonical_url,
            title=title,
            authors=[],
            publisher=publisher,
            publication_date=_parse_iso_date(doc.get("datum")),
            language="de",
            abstract=doc.get("abstract"),
            discovery_query=topic.id,
            raw={
                "dip_id": doc_id,
                "drucksachetyp": doc.get("drucksachetyp"),
                "dokumentart": doc.get("dokumentart"),
                "dokumentnummer": fundstelle.get("dokumentnummer"),
                "wahlperiode": doc.get("wahlperiode"),
                "herausgeber": fundstelle.get("herausgeber"),
                "pdf_url": pdf_url or None,
                "vorgangsbezug": [
                    {"id": v.get("id"), "titel": v.get("titel"),
                     "vorgangstyp": v.get("vorgangstyp")}
                    for v in (doc.get("vorgangsbezug") or [])[:10]
                    if isinstance(v, dict)
                ],
            },
        )


def _search_terms(topic: Topic) -> List[str]:
    """Topic keywords for `f.titel`, german-locale first, deduped.

    Synonyms and include_keywords are INTERLEAVED (syn, incl, syn, incl…):
    synonyms are the broad umbrella terms ("Klima") that match many
    Drucksachen titles, include_keywords are the specific terms the
    question registry depends on (Klimaschutzgesetz, Mietpreisbremse,
    Renteneintrittsalter). A plain synonyms-first ordering let topics
    with many synonyms (klima has 14) fill the whole term budget, so no
    specific term was ever queried (audit 2026-07-11)."""
    terms: List[str] = []
    seen: set = set()

    def _push(term: str) -> None:
        key = term.strip().lower()
        if key and key not in seen:
            seen.add(key)
            terms.append(term.strip())

    locales = sorted(
        topic.locales.items(), key=lambda kv: kv[0] != "de"
    )  # 'de' first — DIP titles are German
    for _lang, locale in locales:
        for syn, incl in zip_longest(locale.synonyms, locale.include_keywords):
            if syn is not None:
                _push(syn)
            if incl is not None:
                _push(incl)
    return terms[:_MAX_TERMS]


def _parse_iso_date(value: Any) -> Optional[date]:
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None
