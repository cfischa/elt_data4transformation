"""Auto-reviewer for borderline 'pending' studies (zero-touch loop).

The pipeline parks studies with 0 < topic-score < 0.2 as `status='pending'`
for optional human review (Q12). The maintainer wants a fully automated
loop — nothing should wait on a person. This module triages the pending
band automatically, coverage-first (A12: bias to keep), so every study
ends up `kept` or `rejected` and flows on to claims/attribution.

Design:
  - `decide(...)` is pure and offline-testable — the whole policy lives
    here. It NEVER returns "skip": the point is zero pending left.
  - `run_auto_review(...)` applies decisions via the existing
    promote_study / reject_study storage methods (which stamp
    `reviewed_by='auto'`), and records the rationale in provenance for
    audit. `dry_run=True` previews without writing.

Rule-based and free (no LLM, no network) — runs anywhere, including the
scheduled attribute Action. An LLM second opinion can be layered later.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from study_scraper.storage import PostgresStorage


LOGGER = logging.getLogger(__name__)

AUTO_REVIEWER = "auto"

# A study needs at least this much text to be considered more than a stub.
_MIN_TEXT = 40
# Below this score AND with no other signal, treat as noise.
_NOISE_SCORE = 0.05
# At/above this score, keep outright (close to the 0.2 keep threshold).
_NEAR_THRESHOLD = 0.1


def decide(
    *,
    title: Optional[str],
    abstract: Optional[str],
    claims_count: int,
    max_score: Optional[float],
) -> Tuple[str, str]:
    """Return (decision, rationale) for one pending study.

    decision is 'kept' or 'rejected' — never 'pending'. Coverage-first:
    keep unless the study is clearly noise.
    """
    text_len = len((title or "") + (abstract or ""))
    score = max_score or 0.0

    if claims_count > 0:
        return "kept", f"has {claims_count} quantitative claim(s)"
    if score >= _NEAR_THRESHOLD:
        return "kept", f"topic score {score:.2f} near keep threshold"
    if text_len < _MIN_TEXT and score < _NOISE_SCORE:
        return "rejected", (
            f"noise: score {score:.2f} and only {text_len} chars of text"
        )
    # Coverage-first default: a borderline study with real text and no
    # disqualifier is kept rather than dropped.
    return "kept", "coverage-first default (borderline, no disqualifier)"


def _max_score(topic_scores: Any) -> Optional[float]:
    if isinstance(topic_scores, dict) and topic_scores:
        try:
            return max(float(v) for v in topic_scores.values())
        except (TypeError, ValueError):
            return None
    return None


def run_auto_review(
    *,
    storage: PostgresStorage,
    limit: int = 100,
    topic: Optional[str] = None,
    dry_run: bool = False,
) -> List[Dict[str, Any]]:
    """Triage pending studies. Returns one result dict per study."""
    rows = storage.list_studies(topic_id=topic, status="pending", limit=limit)
    results: List[Dict[str, Any]] = []
    for row in rows:
        sid = row["id"]
        claims_count = len(storage.claims_for_study(sid))
        decision, rationale = decide(
            title=row.get("title"),
            abstract=row.get("abstract"),
            claims_count=claims_count,
            max_score=_max_score(row.get("topic_scores")),
        )
        if not dry_run:
            storage.set_review_rationale(sid, f"auto: {rationale}")
            if decision == "kept":
                storage.promote_study(sid, reviewed_by=AUTO_REVIEWER)
            else:
                storage.reject_study(
                    sid, reviewed_by=AUTO_REVIEWER, reason=rationale
                )
        results.append({
            "study_id": sid,
            "decision": decision,
            "rationale": rationale,
            "title": (row.get("title") or "")[:80],
            "applied": not dry_run,
        })
        LOGGER.info("auto-review %s -> %s (%s)", sid[:12], decision, rationale)
    return results
