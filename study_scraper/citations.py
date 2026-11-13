"""Extract candidate DOIs / URLs from study full text (citation mining, step 1).

Pure functions only: no I/O, no DB. Wiring into the pipeline is a later step.
"""
from __future__ import annotations

import re
from typing import List

_DOI_RE = re.compile(r"\b(10\.\d{4,9}/[^\s<>\"'\]\)]+)", re.IGNORECASE)
_URL_RE = re.compile(r"https?://[^\s<>\"'\]\)]+", re.IGNORECASE)
_TRAIL = ".,;:"


def _clean(token: str) -> str:
    return token.rstrip(_TRAIL)


def extract_dois(text: str) -> List[str]:
    """Return unique lower-cased DOIs in order of first appearance."""
    seen: dict = {}
    for m in _DOI_RE.finditer(text or ""):
        doi = _clean(m.group(1)).lower()
        if doi:
            seen.setdefault(doi, None)
    return list(seen)


def extract_urls(text: str) -> List[str]:
    """Return unique non-DOI-resolver http(s) URLs in order of appearance."""
    seen: dict = {}
    for m in _URL_RE.finditer(text or ""):
        url = _clean(m.group(0))
        if re.match(r"https?://(dx\.)?doi\.org/", url, re.IGNORECASE):
            continue  # already covered by extract_dois
        seen.setdefault(url, None)
    return list(seen)


_REF_HEADING_RE = re.compile(
    r"^\s*(?:\d+\.?\s*)?(?:literatur(?:verzeichnis)?|quellen(?:verzeichnis)?|references|bibliography)\s*:?\s*$",
    re.IGNORECASE | re.MULTILINE,
)


def references_section(text: str) -> str:
    """Return the text after the last references-style heading, else ''.

    Headings recognised (own line): Literatur(verzeichnis), Quellen(verzeichnis),
    References, Bibliography. Using the last match skips table-of-contents hits.
    """
    matches = list(_REF_HEADING_RE.finditer(text or ""))
    if not matches:
        return ""
    return text[matches[-1].end():].strip()
