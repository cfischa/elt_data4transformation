"""Full-document fetch + statistics extraction (Phase 6-full, A20).

Per maintainer 2026-05-31: the goal is NOT abstract-only — "scanning a
lot of documents, or even better, have all the statistics and
quantitative connections for all the studies which are relevant."

This module closes the loop for each kept study:

    canonical_url → fetch bytes → detect type (PDF / HTML)
                  → extract text → save raw artifact to disk
                  → run claim extractor over the FULL text
                  → store claims under extractor='regex-v2'
                  → set studies.raw_artifact_ref

Claims from the abstract pass (`regex-v1`) and the full-text pass
(`regex-v2`) coexist per study; re-running either replaces only its
own claims (storage.upsert_claims is per-extractor idempotent).

Studies whose full text yields NO claims stay visible via the
`reading_list` view (migration 0007): relevant, but a human needs to
read them — the "quantitative statistics plus studies we need to
read" split the maintainer asked for.

Network use is confined to `fetch_url`; everything else is pure and
testable offline. The CLI command (`study_scraper fulltext`) drives
the live loop on the maintainer's machine.
"""

from __future__ import annotations

import hashlib
import logging
import re
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

from study_scraper.claims import extract_claims_from_text
from study_scraper.config import get_settings
from study_scraper.http import get_with_retry, polite_sleep
from study_scraper.pdf_resolver import is_pdf_url, resolve_pdf_url
from study_scraper.storage import PostgresStorage


LOGGER = logging.getLogger(__name__)

FULLTEXT_EXTRACTOR = "regex-v2"

# raw_artifact_ref marker used when the runtime doesn't persist artifacts
# (e.g. scheduled CI, whose disk vanishes after the job) — proves the
# document WAS fetched/processed without claiming a file exists.
PROCESSED_MARKER_PREFIX = "processed:"

# Hard cap on artifact size — a polling report is rarely >20 MB; bigger
# usually means we hit a data dump or video by mistake.
MAX_ARTIFACT_BYTES = 50 * 1024 * 1024


# ----------------------------------------------------------------------
# Text extraction (pure; no I/O)
# ----------------------------------------------------------------------


def sniff_kind(content: bytes, content_type: Optional[str]) -> str:
    """Classify payload as 'pdf' | 'html' | 'unknown'."""
    if content[:5] == b"%PDF-":
        return "pdf"
    ct = (content_type or "").lower()
    if "pdf" in ct:
        return "pdf"
    head = content[:2048].lstrip().lower()
    if (
        "html" in ct
        or head.startswith(b"<!doctype html")
        or head.startswith(b"<html")
        or b"<html" in head
    ):
        return "html"
    return "unknown"


def extract_text_from_pdf(content: bytes) -> str:
    """Extract text from PDF bytes via pypdf. Best-effort per page."""
    from pypdf import PdfReader  # imported lazily; heavy dependency

    reader = PdfReader(BytesIO(content))
    parts: List[str] = []
    for page in reader.pages:
        try:
            parts.append(page.extract_text() or "")
        except Exception as exc:  # pragma: no cover - malformed page
            LOGGER.warning("pdf page extraction failed: %s", exc)
    return "\n".join(p for p in parts if p).strip()


def extract_text_from_html(content: bytes) -> str:
    """Extract visible text from HTML bytes via BeautifulSoup.

    Drops script/style/nav/footer noise; keeps headings, paragraphs,
    list items, and table cells — the places polling numbers live.
    """
    from bs4 import BeautifulSoup  # imported lazily

    soup = BeautifulSoup(content, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator=" ", strip=True)
    return re.sub(r"\s+", " ", text).strip()


def extract_text(content: bytes, content_type: Optional[str]) -> Tuple[str, str]:
    """Return (kind, text). Empty text when the payload is unusable."""
    kind = sniff_kind(content, content_type)
    if kind == "pdf":
        return kind, extract_text_from_pdf(content)
    if kind == "html":
        return kind, extract_text_from_html(content)
    return kind, ""


# ----------------------------------------------------------------------
# Artifact persistence + claims (DB-touching, but offline-testable)
# ----------------------------------------------------------------------


def process_document(
    *,
    storage: PostgresStorage,
    study_id: str,
    content: bytes,
    content_type: Optional[str] = None,
    artifact_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    """Process one fetched document for one study. Returns a summary.

    Steps: extract text → save artifact → claims (regex-v2) →
    set raw_artifact_ref. Caller provides the bytes (live fetch or
    test fixture); this function never touches the network.
    """
    if len(content) > MAX_ARTIFACT_BYTES:
        return {
            "study_id": study_id, "status": "too_large",
            "bytes": len(content), "claims": 0,
        }

    kind, text = extract_text(content, content_type)
    if not text:
        return {
            "study_id": study_id, "status": f"no_text_({kind})",
            "bytes": len(content), "claims": 0,
        }

    # Persist the raw artifact under sha-named file so re-fetching the
    # same study overwrites rather than accumulates. Skipped in runtimes
    # whose disk doesn't survive the job (STUDY_SCRAPER_PERSIST_ARTIFACTS=
    # false) — a marker records that fulltext ran without claiming a file
    # exists, so the fulltext queue (raw_artifact_ref IS NULL) still works.
    if get_settings().persist_artifacts:
        base_dir = artifact_dir or get_settings().artifact_local_dir
        base_dir.mkdir(parents=True, exist_ok=True)
        ext = "pdf" if kind == "pdf" else "html"
        artifact_path = base_dir / f"{study_id}.{ext}"
        artifact_path.write_bytes(content)
        ref = str(artifact_path)
    else:
        ref = PROCESSED_MARKER_PREFIX + hashlib.sha256(content).hexdigest()

    claims = extract_claims_from_text(study_id=study_id, text=text)
    n = storage.upsert_claims(study_id, claims, extractor=FULLTEXT_EXTRACTOR)
    storage.set_artifact_ref(study_id, ref)

    return {
        "study_id": study_id,
        "status": "ok",
        "kind": kind,
        "bytes": len(content),
        "text_chars": len(text),
        "claims": n,
        "artifact": ref,
    }


# ----------------------------------------------------------------------
# Live loop (network; runs on the maintainer's machine)
# ----------------------------------------------------------------------


def fetch_url(url: str, *, timeout: float = 60.0) -> Tuple[bytes, Optional[str]]:
    """GET one URL; returns (bytes, content_type). Raises on HTTP error."""
    settings = get_settings()
    with httpx.Client(
        timeout=timeout,
        headers={"User-Agent": settings.http_user_agent},
        follow_redirects=True,
    ) as client:
        resp = get_with_retry(client, url)
        resp.raise_for_status()
        return resp.content, resp.headers.get("content-type")


# openalex.org work pages are bot-protected METADATA pages, never the
# document — fetching them 403s (seen live 2026-07-02, 7/91 failures).
_OPENALEX_HOST_RE = re.compile(r"^https?://(www\.)?openalex\.org/", re.I)


def select_fetch_url(
    *,
    canonical_url: Optional[str],
    doi: Optional[str] = None,
    provenance: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[str], str]:
    """Pick the URL fulltext should actually fetch for a study.

    Returns (url, why). Non-openalex canonical URLs pass through
    unchanged. For openalex.org URLs we prefer, in order: the work's
    `pdf_url`, its `landing_page_url` (both captured into provenance at
    ingest), then the DOI. If nothing fetchable exists, (None,
    'no_fetchable_url') — callers should mark the study so it leaves
    the fulltext queue instead of 403-ing forever. Pure; no I/O.
    """
    if not canonical_url or not _OPENALEX_HOST_RE.match(canonical_url):
        return canonical_url, "canonical"
    prov = provenance or {}
    for key in ("pdf_url", "landing_page_url"):
        alt = prov.get(key)
        if isinstance(alt, str) and alt and not _OPENALEX_HOST_RE.match(alt):
            return alt, key
    if doi:
        url = doi if doi.startswith("http") else f"https://doi.org/{doi}"
        return url, "doi"
    return None, "no_fetchable_url"


def run_fulltext(
    *,
    storage: PostgresStorage,
    limit: int = 20,
    study_id: Optional[str] = None,
    refetch: bool = False,
) -> List[Dict[str, Any]]:
    """Fetch + process documents for kept studies missing an artifact.

    `refetch=True` re-processes studies that already have one (e.g.
    after an extractor improvement). Returns one summary dict per
    study attempted.
    """
    if study_id is not None:
        row = storage.get_study(study_id)
        rows = [row] if row else []
    else:
        rows = storage.list_studies_for_fulltext(
            limit=limit, include_done=refetch
        )

    results: List[Dict[str, Any]] = []
    politeness = get_settings().http_politeness_delay_seconds
    for idx, row in enumerate(rows):
        if idx > 0:
            polite_sleep(politeness)  # don't burst all documents at one host
        sid = row["id"]
        url, why = select_fetch_url(
            canonical_url=row.get("canonical_url"),
            doi=row.get("doi"),
            provenance=row.get("provenance") or {},
        )
        if url is None:
            # Nothing fetchable (openalex-only work, no DOI/location):
            # flag it so it leaves the queue instead of erroring forever.
            storage.mark_unfetchable(sid)
            results.append({
                "study_id": sid, "status": "no_fetchable_url", "claims": 0,
                "canonical_url": row.get("canonical_url"),
                "resolved_pdf_url": None,
            })
            LOGGER.info("fulltext %s: no_fetchable_url", sid[:12])
            continue
        resolved_pdf: Optional[str] = None
        try:
            content, content_type = fetch_url(url)
            # If we landed on an HTML page (not the PDF itself), try to
            # resolve the real document and read THAT instead — otherwise
            # we'd extract the landing page's chrome, not the study.
            if sniff_kind(content, content_type) == "html" and not is_pdf_url(url):
                pdf_url = resolve_pdf_url(url, content)
                if pdf_url and pdf_url != url:
                    try:
                        pdf_content, pdf_ct = fetch_url(pdf_url)
                        if sniff_kind(pdf_content, pdf_ct) == "pdf":
                            content, content_type = pdf_content, pdf_ct
                            resolved_pdf = pdf_url
                            storage.set_resolved_pdf_url(sid, pdf_url)
                    except Exception as exc:  # fall back to the landing HTML
                        LOGGER.warning(
                            "resolved pdf fetch failed (%s); using landing page: %s",
                            pdf_url, exc,
                        )
            summary = process_document(
                storage=storage, study_id=sid,
                content=content, content_type=content_type,
            )
        except Exception as exc:
            summary = {
                "study_id": sid, "status": f"fetch_error: {exc}",
                "claims": 0,
            }
            LOGGER.warning("fulltext fetch failed for %s: %s", url, exc)
        summary["canonical_url"] = row.get("canonical_url")
        # Where we actually fetched from, when it wasn't the canonical URL
        # (openalex fallback via pdf_url / landing_page_url / DOI).
        summary["fetch_url"] = url if why != "canonical" else None
        summary["resolved_pdf_url"] = resolved_pdf
        results.append(summary)
        LOGGER.info(
            "fulltext %s: %s (%s claims)%s",
            sid[:12], summary["status"], summary.get("claims", 0),
            f" [resolved {resolved_pdf}]" if resolved_pdf else "",
        )
    return results
