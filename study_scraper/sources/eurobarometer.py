"""Eurobarometer lake source (A14 / issue #35).

Eurobarometer (the European Commission's standing EU opinion survey)
is archived by **GESIS** -- the same public Knowledge Graph SPARQL
endpoint already used by `gesis.py` (`data.gesis.org/gesiskg/sparql`,
no auth, per A15/Q17). Every Eurobarometer wave GESIS holds is a
`schema:Dataset` in that same catalog, so this source is the GESIS
catalog **filtered to titles containing "Eurobarometer"**, emitted
under its own `source_id` so Eurobarometer coverage/license/review
tracking stays independent of the general GESIS ingest.

We deliberately do *not* reuse `GESISSource` directly: its catalog
query has no filter hook, and duplicating the small, source-specific
SPARQL/parsing here matches how DAWUM/Eurostat/GESIS are each
independent modules in this package.

Modes (mirrors DAWUM / Eurostat / GESIS):

  * `from_file=<path>` -- reads a saved JSON document from disk in
    the same wrapper shape as the GESIS fixture. Used by tests and by
    environments without outbound HTTP.
  * Live (default) -- runs SPARQL queries against the public endpoint.

Actual microdata downloads (SPSS/Stata/CSV bytes) require GESIS
account registration -- same deferral as GESIS's own Q19. This source
only harvests the public catalog metadata (title, abstract, wave
number, fielding dates, DOI, license), per A14 "collect as it is
first."
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

try:
    from SPARQLWrapper import JSON as SPARQL_JSON
    from SPARQLWrapper import SPARQLWrapper
except ImportError:  # pragma: no cover - dependency gate
    SPARQL_JSON = None  # type: ignore[assignment]
    SPARQLWrapper = None  # type: ignore[assignment]

from study_scraper.models import SourceRecord


LOGGER = logging.getLogger(__name__)

DEFAULT_ENDPOINT = "https://data.gesis.org/gesiskg/sparql"
EUROBAROMETER_LICENSE = "GESIS terms of use (Eurobarometer data archive)"
RECORD_FORMAT = "eurobarometer_kg_sparql_json"

# Same catalog as GESISSource, filtered to Eurobarometer waves by title.
_CATALOG_QUERY = """
PREFIX schema: <https://schema.org/>
SELECT DISTINCT ?d WHERE {
  ?d a schema:Dataset ;
     schema:name ?name .
  FILTER(CONTAINS(LCASE(STR(?name)), "eurobarometer"))
}
ORDER BY ?d
LIMIT %d OFFSET %d
"""

_BATCH_DETAIL_QUERY = """
PREFIX schema: <https://schema.org/>
SELECT ?d ?p ?o WHERE {
  VALUES ?d { %s }
  ?d a schema:Dataset ;
     ?p ?o .
}
"""


class EurobarometerSource:
    """Lake source for Eurobarometer waves in the GESIS Knowledge Graph."""

    source_id = "eurobarometer"

    def __init__(
        self,
        *,
        endpoint: str = DEFAULT_ENDPOINT,
        from_file: Optional[Path] = None,
        page_size: int = 200,
        batch_size: int = 20,
        user_agent: str = "study-scraper/0.0.1 (+https://github.com/cfischa/elt_data4transformation)",
    ) -> None:
        self._endpoint = endpoint
        self._from_file = from_file
        self._page_size = page_size
        self._batch_size = batch_size
        self._user_agent = user_agent

    def __enter__(self) -> "EurobarometerSource":
        return self

    def __exit__(self, *exc: object) -> None:
        pass

    # ------------------------------------------------------------------
    # Public iterator
    # ------------------------------------------------------------------

    def iter_records(
        self,
        *,
        run_id: str,
        limit: Optional[int] = None,
    ) -> Iterator[SourceRecord]:
        """Yield one SourceRecord per Eurobarometer `schema:Dataset` URI.

        Live mode: paginate the filtered catalog, batch-fetch details,
        emit. Fixture mode: read a saved JSON document, emit.
        """
        if self._from_file is not None:
            yield from self._iter_from_file(run_id=run_id, limit=limit)
            return

        yield from self._iter_live(run_id=run_id, limit=limit)

    # ------------------------------------------------------------------
    # Fixture path -- mirrors live shape so tests exercise the parser
    # ------------------------------------------------------------------

    def _iter_from_file(
        self,
        *,
        run_id: str,
        limit: Optional[int],
    ) -> Iterator[SourceRecord]:
        assert self._from_file is not None
        raw = json.loads(self._from_file.read_text(encoding="utf-8"))
        # File layout:
        # {"datasets": [{"uri": "...",
        #                "triples": [{"p": "...", "o": "...",
        #                             "type": "literal|uri",
        #                             "lang": "...", "datatype": "..."},
        #                            ...]},
        #               ...]}
        now = datetime.now(timezone.utc)
        yielded = 0
        for entry in raw.get("datasets") or []:
            uri = (entry.get("uri") or "").strip()
            triples = entry.get("triples") or []
            if not uri or not triples:
                continue
            record = self._build_record(
                uri=uri, triples=triples, now=now, run_id=run_id,
            )
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
        if SPARQLWrapper is None:
            raise RuntimeError(
                "SPARQLWrapper is not installed; add it to your "
                "environment to use the live Eurobarometer source."
            )
        now = datetime.now(timezone.utc)
        offset = 0
        yielded = 0
        while True:
            uris = self._fetch_catalog_page(offset=offset)
            if not uris:
                return
            LOGGER.info(
                "eurobarometer catalog: offset=%d size=%d",
                offset, len(uris),
            )
            for batch_start in range(0, len(uris), self._batch_size):
                batch = uris[batch_start:batch_start + self._batch_size]
                details = self._fetch_batch_details(batch)
                for uri in batch:
                    triples = details.get(uri) or []
                    if not triples:
                        continue
                    record = self._build_record(
                        uri=uri, triples=triples, now=now, run_id=run_id,
                    )
                    if record is None:
                        continue
                    yield record
                    yielded += 1
                    if limit is not None and yielded >= limit:
                        return
            offset += self._page_size

    def _new_wrapper(self) -> Any:
        assert SPARQLWrapper is not None
        sw = SPARQLWrapper(self._endpoint, agent=self._user_agent)
        sw.setReturnFormat(SPARQL_JSON)
        return sw

    def _fetch_catalog_page(self, *, offset: int) -> List[str]:
        sw = self._new_wrapper()
        sw.setQuery(_CATALOG_QUERY % (self._page_size, offset))
        results = sw.query().convert()
        return [
            b["d"]["value"]
            for b in (results.get("results") or {}).get("bindings") or []
            if "d" in b and "value" in b["d"]
        ]

    def _fetch_batch_details(
        self, uris: List[str]
    ) -> Dict[str, List[Dict[str, Any]]]:
        sw = self._new_wrapper()
        values = " ".join(f"<{u}>" for u in uris)
        sw.setQuery(_BATCH_DETAIL_QUERY % values)
        results = sw.query().convert()
        out: Dict[str, List[Dict[str, Any]]] = {}
        for b in (results.get("results") or {}).get("bindings") or []:
            uri = (b.get("d") or {}).get("value")
            p = (b.get("p") or {}).get("value")
            o_node = b.get("o") or {}
            if not uri or not p:
                continue
            out.setdefault(uri, []).append({
                "p": p,
                "o": o_node.get("value"),
                "type": o_node.get("type"),         # uri / literal / bnode
                "lang": o_node.get("xml:lang"),
                "datatype": o_node.get("datatype"),
            })
        return out

    # ------------------------------------------------------------------
    # Record construction (shared by live + fixture)
    # ------------------------------------------------------------------

    def _build_record(
        self,
        *,
        uri: str,
        triples: List[Dict[str, Any]],
        now: datetime,
        run_id: str,
    ) -> Optional[SourceRecord]:
        title = _first_literal(triples, _TITLE_PROPS)
        license_uri = _first_value(triples, _LICENSE_PROPS)
        doi = _first_doi(triples)

        # Stable hash of the (URI + sorted triples) so re-running on
        # unchanged upstream data hashes the same way.
        payload = {
            "uri": uri,
            "triples": sorted(
                triples, key=lambda t: (t.get("p") or "", str(t.get("o")))
            ),
        }
        payload_bytes = json.dumps(
            payload, ensure_ascii=False, sort_keys=True
        ).encode("utf-8")
        content_hash = hashlib.sha256(payload_bytes).hexdigest()

        source_record_id = _extract_source_record_id(uri)

        return SourceRecord.build(
            source_id=self.source_id,
            source_record_id=source_record_id,
            canonical_url=uri,
            format=RECORD_FORMAT,
            content_type="application/sparql-results+json",
            content_hash=content_hash,
            fetched_at=now,
            discovery_run_id=run_id,
            payload=payload,
            doi=doi,
            license=license_uri or EUROBAROMETER_LICENSE,
            provenance={
                "fetch_source": "eurobarometer_gesis_kg_sparql",
                "endpoint": self._endpoint,
                "from_file": (
                    str(self._from_file) if self._from_file else None
                ),
                "title_hint": title,
            },
        )


# ----------------------------------------------------------------------
# Helpers (free functions so tests can target them without the class)
# ----------------------------------------------------------------------

_TITLE_PROPS = (
    "https://schema.org/name",
    "http://schema.org/name",
    "http://purl.org/dc/terms/title",
)
_LICENSE_PROPS = (
    "https://schema.org/license",
    "http://schema.org/license",
    "http://purl.org/dc/terms/license",
)


def _first_value(triples: List[Dict[str, Any]], predicates) -> Optional[str]:
    for t in triples:
        if t.get("p") in predicates and t.get("o"):
            return str(t["o"])
    return None


def _first_literal(triples: List[Dict[str, Any]], predicates) -> Optional[str]:
    for t in triples:
        if (
            t.get("p") in predicates
            and t.get("o")
            and t.get("type") == "literal"
        ):
            return str(t["o"])
    return None


def _first_doi(triples: List[Dict[str, Any]]) -> Optional[str]:
    """Find a DOI in the triples. GESIS exposes DOIs via several
    predicates; check known ones plus the URL prefix.
    """
    doi_props = (
        "https://schema.org/sameAs",
        "https://schema.org/identifier",
        "http://purl.org/dc/terms/identifier",
    )
    for t in triples:
        val = t.get("o")
        if not isinstance(val, str):
            continue
        if t.get("p") in doi_props or "doi" in (t.get("p") or "").lower():
            if "doi.org/" in val:
                return val.split("doi.org/", 1)[1].strip()
            if val.lower().startswith("doi:"):
                return val[4:].strip()
            # Some GESIS records expose a bare 10.x/y string.
            if val.startswith("10.") and "/" in val:
                return val.strip()
    return None


def _extract_source_record_id(uri: str) -> Optional[str]:
    """Last URI segment -- works for `.../resource/ZA6900` style URIs."""
    if not uri:
        return None
    bare = uri.rstrip("/")
    if "#" in bare:
        bare = bare.split("#")[-1]
    if "/" in bare:
        bare = bare.rsplit("/", 1)[-1]
    return bare or None
