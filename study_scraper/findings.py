"""Cross-study dedup of survey findings (confidence-weighted, read-time).

The same finding ("62% support a climate law") can appear across several
studies/attributions — the same poll cited twice, or two write-ups of one
survey. For the answer layer we want ONE representative per distinct
finding, not N near-duplicates.

This is done at READ time over the rows `search_attributions` returns:
raw attribution rows stay untouched (full audit trail), dedup is a pure
function applied before display. No migration, fully offline-testable.

A finding's identity = (normalized question, position, percentage rounded
to the nearest whole point). Within a group we keep the highest-confidence
row (ties broken by higher percentage, then by presence of a grounded
source span), and report how many duplicates it stood for.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple


def _norm_question(q: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (q or "").lower()).strip()


def dedup_finding_key(
    question: Optional[str],
    position: Optional[str],
    percentage: Optional[float],
) -> Tuple[str, str, Optional[int]]:
    """Identity of a finding for dedup. Percentage rounds to the nearest
    whole point so 61.6% and 62% collapse; None stays distinct."""
    pos = (position or "unspecified").strip().lower()
    pct: Optional[int]
    if percentage is None:
        pct = None
    else:
        try:
            pct = int(round(float(percentage)))
        except (TypeError, ValueError):
            pct = None
    return (_norm_question(question), pos, pct)


def _confidence(row: Dict[str, Any]) -> float:
    c = row.get("confidence")
    try:
        return float(c) if c is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _percentage(row: Dict[str, Any]) -> float:
    p = row.get("percentage")
    try:
        return float(p) if p is not None else -1.0
    except (TypeError, ValueError):
        return -1.0


def _grounded(row: Dict[str, Any]) -> int:
    raw = row.get("raw")
    if isinstance(raw, dict) and raw.get("grounded") is True:
        return 1
    return 0


def dedupe_attributions(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Collapse rows to one representative per finding key.

    Representative = max by (confidence, grounded, percentage). Each kept
    row gains `dup_count` (how many rows shared its key, incl. itself).
    Input order of distinct findings is preserved (first appearance wins
    the slot), so an already-sorted list stays sorted.
    """
    best: Dict[Tuple[str, str, Optional[int]], Dict[str, Any]] = {}
    counts: Dict[Tuple[str, str, Optional[int]], int] = {}
    order: List[Tuple[str, str, Optional[int]]] = []

    for row in rows:
        key = dedup_finding_key(
            row.get("question"), row.get("position"), row.get("percentage")
        )
        counts[key] = counts.get(key, 0) + 1
        if key not in best:
            best[key] = row
            order.append(key)
            continue
        cur = best[key]
        challenger = (_confidence(row), _grounded(row), _percentage(row))
        incumbent = (_confidence(cur), _grounded(cur), _percentage(cur))
        if challenger > incumbent:
            best[key] = row

    out: List[Dict[str, Any]] = []
    for key in order:
        rep = dict(best[key])
        rep["dup_count"] = counts[key]
        out.append(rep)
    return out
