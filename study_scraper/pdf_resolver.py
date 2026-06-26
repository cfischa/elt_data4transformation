"""Landing-page → PDF resolver (next-build item 5).

Many `canonical_url`s are HTML *landing pages* (SSOAR handle pages,
journal abstract pages) that link the actual PDF rather than being the
PDF. Without this, `fulltext.py` extracts the landing page's chrome
text instead of the document — the documented top coverage/accuracy gap
(RUNBOOK §3). This module finds the real PDF link inside landing HTML.

Design:
  - **Pure + offline.** `resolve_pdf_url(base_url, html)` takes the page
    HTML and returns an absolute PDF URL (or None). No network, no DB —
    fully unit-testable with fixtures. `fulltext.py` does the fetching.
  - **Strategy order (most authoritative first):**
      1. `<meta name="citation_pdf_url">` — the scholarly standard.
      2. SSOAR/DSpace `/bitstream/.../*.pdf` links.
      3. Any `<a href="*.pdf">`, ranked: prefer full-text names
         (volltext/fulltext/gesamt/full), deprioritize appendices.
  - Relative hrefs are resolved against `base_url`.
  - Robust to malformed HTML (BeautifulSoup + a regex fallback).
"""

from __future__ import annotations

import re
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse


# Tokens that signal the *main* document vs. side material, applied to the
# href + link text to rank candidate PDFs (higher = more likely the doc).
_PREFER = (
    "volltext", "fulltext", "full-text", "full_text", "fulltext",
    "gesamt", "gesamttext", "full", "complete", "report", "bericht",
    "studie", "document", "dokument", "paper",
)
_DEPRIORITIZE = (
    "anhang", "appendix", "supplement", "zusatz", "annex", "cover",
    "abstract", "zusammenfassung", "toc", "inhaltsverzeichnis",
    "erratum", "correction", "poster", "slides", "praesentation",
)

_PDF_HREF_RE = re.compile(r'href=["\']([^"\']+?\.pdf(?:\?[^"\']*)?)["\']', re.I)
_CITATION_META_RE = re.compile(
    r'<meta[^>]+name=["\']citation_pdf_url["\'][^>]*content=["\']([^"\']+)["\']',
    re.I,
)
_CITATION_META_RE_REV = re.compile(
    r'<meta[^>]+content=["\']([^"\']+)["\'][^>]*name=["\']citation_pdf_url["\']',
    re.I,
)


def _as_text(html: object) -> str:
    if isinstance(html, bytes):
        return html.decode("utf-8", errors="replace")
    return str(html or "")


def _absolutize(base_url: str, href: str) -> str:
    href = href.strip()
    return urljoin(base_url, href)


def _looks_like_pdf(url: str) -> bool:
    path = urlparse(url).path.lower()
    return path.endswith(".pdf") or ".pdf?" in url.lower() or "/bitstream/" in url.lower()


def _score_candidate(href: str, text: str) -> int:
    """Rank a candidate PDF link. Higher is better."""
    blob = f"{href} {text}".lower()
    score = 0
    if "/bitstream/" in blob:          # DSpace/SSOAR full-document path
        score += 5
    for tok in _PREFER:
        if tok in blob:
            score += 3
            break
    for tok in _DEPRIORITIZE:
        if tok in blob:
            score -= 4
            break
    # A bare ".pdf" with no obvious side-material marker is a fine default.
    return score


def _meta_citation_pdf(html: str) -> Optional[str]:
    m = _CITATION_META_RE.search(html) or _CITATION_META_RE_REV.search(html)
    return m.group(1) if m else None


def _anchor_candidates(html: str) -> List[Tuple[str, str]]:
    """Return (href, link_text) pairs for every PDF-ish anchor.

    Uses BeautifulSoup when available (captures link text for ranking);
    falls back to a regex over hrefs for malformed HTML.
    """
    out: List[Tuple[str, str]] = []
    try:
        from bs4 import BeautifulSoup  # lazy: heavy, optional at import time

        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.lower().endswith(".pdf") or ".pdf?" in href.lower() \
               or "/bitstream/" in href.lower():
                out.append((href, a.get_text(" ", strip=True) or ""))
    except Exception:
        out = []
    if not out:  # regex fallback (also covers BeautifulSoup-absent envs)
        for href in _PDF_HREF_RE.findall(html):
            out.append((href, ""))
    return out


def resolve_pdf_url(base_url: str, html: object) -> Optional[str]:
    """Find the best PDF URL in a landing page. Absolute URL or None.

    `base_url` is the page's own URL (for resolving relative links);
    `html` is its HTML (str or bytes). Pure: no network, no I/O.
    """
    text = _as_text(html)
    if not text.strip():
        return None

    # 1) citation_pdf_url meta — authoritative when present.
    meta = _meta_citation_pdf(text)
    if meta:
        return _absolutize(base_url, meta)

    # 2/3) rank all PDF anchors; bitstream + full-text names win.
    candidates = _anchor_candidates(text)
    best: Optional[str] = None
    best_score = -(10**9)
    for href, link_text in candidates:
        abs_url = _absolutize(base_url, href)
        if not _looks_like_pdf(abs_url):
            continue
        score = _score_candidate(href, link_text)
        if score > best_score:
            best_score, best = score, abs_url
    return best


def is_pdf_url(url: str) -> bool:
    """True if a URL is almost certainly already a direct PDF (skip resolving)."""
    return _looks_like_pdf(url)
