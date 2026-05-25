"""Stage-1 rule-based topic filter.

This is a port of the matching logic in `pipeline/topic_classifier.py`,
decoupled from ClickHouse and adapted to free-text study artifacts:

- The legacy classifier scores dataset metadata where one strong
  `dataset_hint` already pushes a record over the 0.5 acceptance bar.
  Studies in SSOAR/OpenAlex have no equivalent of dataset_hints, so the
  default acceptance threshold here is lower (0.2 = a single include
  term in the text).
- The caller can tune `min_score` per source or per topic if needed.
- Stage-2 semantic relevance (sentence embeddings) is a separate module
  introduced in Phase 6; this module is intentionally rules-only.

The output is a `MatchResult` with the matched terms and a rationale so
the reason for keeping or dropping a candidate is auditable downstream.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Optional

from study_scraper.topics import Topic


DEFAULT_MIN_SCORE = 0.2


@dataclass
class MatchResult:
    topic_id: str
    score: float
    matched_includes: List[str] = field(default_factory=list)
    matched_synonyms: List[str] = field(default_factory=list)
    matched_excludes: List[str] = field(default_factory=list)

    @property
    def passes(self) -> bool:
        return not self.matched_excludes and self.score > 0.0

    @property
    def rationale(self) -> str:
        if self.matched_excludes:
            return f"excluded: {', '.join(self.matched_excludes)}"
        terms = self.matched_includes + self.matched_synonyms
        return f"matched: {', '.join(terms)}" if terms else "no matches"


def _gather_terms(topic: Topic, attr: str) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for locale in topic.locales.values():
        for term in getattr(locale, attr):
            key = term.strip().lower()
            if key and key not in seen:
                seen.add(key)
                out.append(key)
    return out


def score_text(text: str, topic: Topic) -> MatchResult:
    """Compute a single-topic match for one piece of text.

    The score combines:
      - up to 0.4 from include keywords (0.2 per match, capped at 0.4),
      - up to 0.2 from synonym keywords (0.1 per match, capped at 0.2).
    Exclude keywords short-circuit to a zero score and a non-empty
    `matched_excludes`.
    """
    blob = (text or "").lower()
    excludes = [t for t in _gather_terms(topic, "exclude_keywords") if t in blob]
    if excludes:
        return MatchResult(topic_id=topic.id, score=0.0, matched_excludes=excludes)

    includes = [t for t in _gather_terms(topic, "include_keywords") if t in blob]
    synonyms = [t for t in _gather_terms(topic, "synonyms") if t in blob]

    # If an include keyword already matched, count the synonyms too (they
    # can boost the score), but don't double-count terms that appear in
    # both lists.
    synonyms = [s for s in synonyms if s not in includes]

    score = 0.0
    score += min(0.4, 0.2 * len(includes))
    score += min(0.2, 0.1 * len(synonyms))
    score = min(1.0, score)
    return MatchResult(
        topic_id=topic.id,
        score=score,
        matched_includes=includes,
        matched_synonyms=synonyms,
    )


def classify(
    text: str,
    topics: Iterable[Topic],
    *,
    min_score: float = DEFAULT_MIN_SCORE,
) -> List[MatchResult]:
    """Return MatchResults sorted by descending score, filtered by `min_score`.

    Excludes are checked per-topic only; an exclude hit on one topic does
    not affect another topic's match.
    """
    results: List[MatchResult] = []
    for topic in topics:
        match = score_text(text, topic)
        if not match.passes:
            continue
        if match.score >= min_score:
            results.append(match)
    results.sort(key=lambda r: r.score, reverse=True)
    return results
