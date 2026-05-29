"""Run one discovery source against one topic, end-to-end.

Steps per candidate:
    1. Source yields a `Candidate`.
    2. `topic_filter.score_text` decides whether to keep it (stage-1
       rules); excluded candidates are dropped.
    3. The candidate is promoted to a `Study` and upserted into
       Postgres.
    4. A `CrawlRun` records aggregate counters and is attached to all
       kept studies via `crawl_run_studies`.

Stage-2 semantic filtering, raw-artifact downloads (PDF/HTML), and full
key-findings extraction are deferred to later phases. The Study row
captured here is sufficient for the maintainer to verify topical
relevance from title + abstract.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Iterable, List, Optional

from study_scraper.claims import extract_claims
from study_scraper.discovery.base import Candidate, DiscoverySource
from study_scraper.models import CrawlRun, Provenance, Study
from study_scraper.storage import PostgresStorage
from study_scraper.topic_filter import MatchResult, score_text
from study_scraper.topics import Topic


LOGGER = logging.getLogger(__name__)


def _has_quantitative_signal(text: str) -> bool:
    """Cheap heuristic for `Study.has_quantitative_data`.

    Looks for digit-percent patterns and sample-size phrasings common in
    German social-science studies. False positives and false negatives
    are both fine here — the flag is a hint for step 2 filtering, not a
    gate.
    """
    if not text:
        return False
    lower = text.lower()
    cues = [
        "%",
        "prozent",
        "percent",
        "n=",
        "n =",
        "stichprobe",
        "sample size",
        "befragt",
        "befragte",
        "respondents",
        "umfrage",
        "survey",
        "repräsentativ",
        "representative",
        "regression",
    ]
    return any(cue in lower for cue in cues)


def _candidate_to_study(
    cand: Candidate,
    match: MatchResult,
    *,
    now: datetime,
) -> Study:
    text = " ".join(filter(None, [cand.title, cand.abstract])).strip()
    provenance = Provenance(
        discovery_source=cand.source_id,
        discovery_query=cand.discovery_query,
        extractor_version="phase4-v1",
    )
    return Study.build(
        canonical_url=cand.canonical_url,
        source_urls=[cand.canonical_url],
        title=cand.title,
        authors=list(cand.authors),
        publisher=cand.publisher,
        publication_date=cand.publication_date,
        language=cand.language,
        doi=_normalize_doi(cand.doi),
        topic_ids=[match.topic_id],
        topic_scores={match.topic_id: round(match.score, 3)},
        has_quantitative_data=_has_quantitative_signal(text),
        abstract=cand.abstract,
        fetched_at=now,
        source_id=cand.source_id,
        provenance=provenance,
    )


def _normalize_doi(value: Optional[str]) -> Optional[str]:
    """Normalize different DOI surface forms to a bare `10.xxx/yyy`.

    Inputs we see: "https://doi.org/10.1515/pwp-2023-0031",
    "doi:10.1515/pwp-2023-0031", "10.1515/pwp-2023-0031".
    """
    if not value:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    lower = stripped.lower()
    for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
        if lower.startswith(prefix):
            stripped = stripped[len(prefix):]
            break
    return stripped or None


def run_one(
    *,
    source: DiscoverySource,
    topic: Topic,
    storage: PostgresStorage,
    limit: Optional[int] = None,
    min_score: float = 0.2,
) -> CrawlRun:
    """Run one source × topic to completion. Returns the recorded CrawlRun."""
    run = CrawlRun(
        id=str(uuid.uuid4()),
        source_id=source.source_id,
        topic_id=topic.id,
        started_at=datetime.now(timezone.utc),
        parameters={"limit": limit, "min_score": min_score},
    )

    kept_ids: List[tuple[str, bool]] = []
    pending_count = 0
    seen = 0
    errors = 0

    try:
        for cand in source.iter_candidates(topic, limit=limit):
            seen += 1
            try:
                text = " ".join(filter(None, [cand.title, cand.abstract]))
                match = score_text(text, topic)
                # Three outcomes per Q12 (DECISIONS.md A12 + Q12 yes):
                #   - excludes hit OR score == 0 => not stored at all
                #   - 0 < score < min_score      => stored as 'pending'
                #     (review queue in the dock)
                #   - score >= min_score         => stored as 'kept'
                if not match.passes or match.score <= 0.0:
                    LOGGER.debug(
                        "dropped (%s): %s — %s",
                        cand.canonical_url,
                        match.score,
                        match.rationale,
                    )
                    continue
                study = _candidate_to_study(cand, match, now=run.started_at)
                claims = extract_claims(
                    study_id=study.id,
                    title=study.title,
                    abstract=study.abstract,
                )
                if match.score < min_score:
                    storage.upsert_study(study, status="pending")
                    storage.upsert_claims(study.id, claims)
                    pending_count += 1
                    LOGGER.info(
                        "pending (%.2f): %d claim(s): %s — %s",
                        match.score,
                        len(claims),
                        cand.title[:80],
                        cand.canonical_url,
                    )
                    continue
                is_new = storage.upsert_study(study, status="kept")
                storage.upsert_claims(study.id, claims)
                kept_ids.append((study.id, is_new))
                LOGGER.info(
                    "kept (%.2f): %d claim(s): %s — %s",
                    match.score,
                    len(claims),
                    cand.title[:80],
                    cand.canonical_url,
                )
            except Exception as exc:  # pragma: no cover - logged + counted
                errors += 1
                LOGGER.exception(
                    "error processing candidate %s: %s",
                    getattr(cand, "canonical_url", "<unknown>"),
                    exc,
                )
    finally:
        params_out = dict(run.parameters)
        params_out["pending"] = pending_count
        run = run.model_copy(
            update={
                "finished_at": datetime.now(timezone.utc),
                "candidates_seen": seen,
                "candidates_kept": len(kept_ids),
                "errors": errors,
                "parameters": params_out,
            }
        )
        storage.record_crawl_run(run)
        if kept_ids:
            storage.attach_studies_to_run(run.id, kept_ids)

    LOGGER.info(
        "run %s done: seen=%d kept=%d errors=%d",
        run.id,
        run.candidates_seen,
        run.candidates_kept,
        run.errors,
    )
    return run
