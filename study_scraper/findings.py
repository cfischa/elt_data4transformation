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
row (ties broken by grounded source span, then newer publication date,
then higher percentage), and report how many duplicates it stood for.
"""

from __future__ import annotations

import datetime as _dt
import re
from typing import Any, Dict, List, Optional, Tuple


def _norm_question(q: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (q or "").lower()).strip()


def normalize_population(population: Optional[str]) -> str:
    """Canonical population identity ('' = general/unlabeled). Shared by
    dedup AND aggregation so both agree on what counts as the same
    population — dedup treating 'Ostdeutsche' as distinct while the
    aggregate pooled it into one mean was inconsistent (audit 2026-07-11)."""
    return re.sub(r"\s+", " ", (population or "").lower()).strip()


def dedup_finding_key(
    question: Optional[str],
    position: Optional[str],
    percentage: Optional[float],
    population: Optional[str] = None,
) -> Tuple[str, str, Optional[int], str]:
    """Identity of a finding for dedup. Percentage rounds to the nearest
    whole point so 61.6% and 62% collapse; None stays distinct.

    `population` is part of the identity: the same question+% among
    "Ostdeutsche" and among "Wahlberechtigte ab 18" are DIFFERENT
    findings and must not merge (statistical-correctness fix,
    2026-07-04). None/blank populations normalize to "" so unlabeled
    findings still dedup against each other.
    """
    pos = (position or "unspecified").strip().lower()
    pct: Optional[int]
    if percentage is None:
        pct = None
    else:
        try:
            pct = int(round(float(percentage)))
        except (TypeError, ValueError):
            pct = None
    return (_norm_question(question), pos, pct, normalize_population(population))


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


def _pub_date(row: Dict[str, Any]) -> _dt.date:
    """Publication date for recency tie-breaks; unknown sorts oldest
    (ROADMAP item B: on confidence ties the newer poll represents the
    finding — a 2025 topline beats a 2021 one saying the same thing)."""
    d = row.get("publication_date")
    if isinstance(d, _dt.datetime):
        return d.date()
    if isinstance(d, _dt.date):
        return d
    return _dt.date.min


def dedupe_attributions(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Collapse rows to one representative per finding key.

    Representative = max by (confidence, grounded, publication_date,
    percentage). Each kept
    row gains `dup_count` (how many rows shared its key, incl. itself).
    Input order of distinct findings is preserved (first appearance wins
    the slot), so an already-sorted list stays sorted.
    """
    best: Dict[Tuple[str, str, Optional[int], str], Dict[str, Any]] = {}
    counts: Dict[Tuple[str, str, Optional[int], str], int] = {}
    order: List[Tuple[str, str, Optional[int], str]] = []

    for row in rows:
        key = dedup_finding_key(
            row.get("question"), row.get("position"),
            row.get("percentage"), row.get("population"),
        )
        counts[key] = counts.get(key, 0) + 1
        if key not in best:
            best[key] = row
            order.append(key)
            continue
        cur = best[key]
        challenger = (
            _confidence(row), _grounded(row), _pub_date(row), _percentage(row)
        )
        incumbent = (
            _confidence(cur), _grounded(cur), _pub_date(cur), _percentage(cur)
        )
        if challenger > incumbent:
            best[key] = row

    out: List[Dict[str, Any]] = []
    for key in order:
        rep = dict(best[key])
        rep["dup_count"] = counts[key]
        out.append(rep)
    return out
