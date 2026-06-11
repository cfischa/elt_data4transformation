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

import logging
import re
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

from study_scraper.claims import extract_claims_from_text
from study_scraper.config import get_settings
from study_scraper.storage import PostgresStorage


LOGGER = logging.getLogger(__name__)

FULLTEXT_EXTRACTOR = "regex-v2"

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
    # same study overwrites rather than accumulates.
    base_dir = artifact_dir or get_settings().artifact_local_dir
    base_dir.mkdir(parents=True, exist_ok=True)
    ext = "pdf" if kind == "pdf" else "html"
    artifact_path = base_dir / f"{study_id}.{ext}"
    artifact_path.write_bytes(content)

    claims = extract_claims_from_text(study_id=study_id, text=text)
    n = storage.upsert_claims(study_id, claims, extractor=FULLTEXT_EXTRACTOR)
    storage.set_artifact_ref(study_id, str(artifact_path))

    return {
        "study_id": study_id,
        "status": "ok",
        "kind": kind,
        "bytes": len(content),
        "text_chars": len(text),
        "claims": n,
        "artifact": str(artifact_path),
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
        resp = client.get(url)
        resp.raise_for_status()
        return resp.content, resp.headers.get("content-type")


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
    for row in rows:
        sid = row["id"]
        url = row["canonical_url"]
        try:
            content, content_type = fetch_url(url)
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
        summary["canonical_url"] = url
        results.append(summary)
        LOGGER.info(
            "fulltext %s: %s (%s claims)",
            sid[:12], summary["status"], summary.get("claims", 0),
        )
    return results
