"""Attribution orchestrator (Option A / A21).

Runs the llm-v1 extractor over the `attribution_queue` (kept studies
that have regex claims but no attribution yet). Two modes:

  - **live** (`run_attribute`) — calls the Anthropic API per study via
    `extractors.llm_v1.extract_live`. Needs `ANTHROPIC_API_KEY`.
  - **offline** (`apply_responses`) — takes captured model responses
    {study_id: response_text} and runs them through the pure parser.
    This is the zero-extra-API-cost path: a Cowork session (or any
    Claude chat) generates the responses, you feed them back here.
    Also the test path.

Both write to `attributions` via `storage.upsert_attributions` (per-model
idempotent).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set

from study_scraper.extractors import llm_v1
from study_scraper.storage import PostgresStorage


LOGGER = logging.getLogger(__name__)

# The queue view has no LIMIT-independent notion of priority, so we scan
# well beyond the requested batch size before reordering — otherwise a
# plain `LIMIT limit` would already have cut the registry-topic studies
# out before prioritize_queue ever sees them.
_QUEUE_SCAN_FLOOR = 500


def build_prompt_for_study(study: Dict[str, Any]) -> str:
    """The exact user prompt llm-v1 would send for this study.

    Exposed so the offline/Cowork path can dump prompts for a human (or
    a Cowork agent) to answer, then feed the responses back.
    """
    return llm_v1.build_user_prompt(
        title=study.get("title"),
        abstract=study.get("abstract"),
        claim_snippets=study.get("claim_snippets") or [],
    )


def run_attribute(
    *,
    storage: PostgresStorage,
    limit: int = 20,
    study_id: Optional[str] = None,
    model: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Live mode: extract attributions for queued studies via the API."""
    study_ids = _target_ids(storage, limit=limit, study_id=study_id)
    results: List[Dict[str, Any]] = []
    for sid in study_ids:
        study = storage.get_study_for_attribution(sid)
        if study is None:
            results.append({"study_id": sid, "status": "not_found", "attributions": 0})
            continue
        try:
            attrs = llm_v1.extract_live(
                study_id=sid,
                title=study.get("title"),
                abstract=study.get("abstract"),
                claim_snippets=study.get("claim_snippets") or [],
                model=model,
            )
            n = storage.upsert_attributions(sid, attrs, model=llm_v1.EXTRACTOR_ID)
            results.append({"study_id": sid, "status": "ok", "attributions": n})
            LOGGER.info("attributed %s: %d triples", sid[:12], n)
        except Exception as exc:  # pragma: no cover - network/credential errors
            results.append({"study_id": sid, "status": f"error: {exc}", "attributions": 0})
            LOGGER.warning("attribution failed for %s: %s", sid[:12], exc)
    return results


def dump_prompts(
    *,
    storage: PostgresStorage,
    limit: int = 20,
    study_id: Optional[str] = None,
) -> List[Dict[str, str]]:
    """Offline step 1: emit {study_id, prompt} for queued studies so a
    Cowork session can answer them. The system prompt is constant —
    `llm_v1.SYSTEM_PROMPT` — and printed once by the caller."""
    out: List[Dict[str, str]] = []
    for sid in _target_ids(storage, limit=limit, study_id=study_id):
        study = storage.get_study_for_attribution(sid)
        if study is None:
            continue
        out.append({"study_id": sid, "prompt": build_prompt_for_study(study)})
    return out


def apply_responses(
    *,
    storage: PostgresStorage,
    responses: Dict[str, str],
    model: str = llm_v1.EXTRACTOR_ID,
) -> List[Dict[str, Any]]:
    """Offline step 2: parse {study_id: response_text} and store.

    No network, no API key. The responses can come from a Cowork
    session, a batch job, or anything that produced JSON matching the
    llm-v1 schema.
    """
    results: List[Dict[str, Any]] = []
    for sid, text in responses.items():
        # Fetch the source text so F1 grounding works on the offline path
        # too (verify each source_span is real). Best-effort: skip if the
        # study isn't found.
        study = storage.get_study_for_attribution(sid)
        source_text = (
            llm_v1.build_user_prompt(
                title=study.get("title"),
                abstract=study.get("abstract"),
                claim_snippets=study.get("claim_snippets") or [],
            )
            if study else None
        )
        attrs = llm_v1.parse_response(
            text, study_id=sid, model=model, source_text=source_text,
        )
        n = storage.upsert_attributions(sid, attrs, model=model)
        results.append({"study_id": sid, "status": "ok", "attributions": n})
    return results


def prioritize_queue(
    rows: List[Dict[str, Any]], registry_topic_ids: Set[str]
) -> List[Dict[str, Any]]:
    """Stable-sort `attribution_queue` rows so studies whose `topic_ids`
    intersect the question registry come first; question-less topics
    fill the remainder in their existing (recency) order.

    A scheduled run only ever attributes a fixed-size batch, so without
    this a topic nobody has a registered question for (e.g. steuern,
    bildung) competes for the same LLM budget as a topic someone is
    actually waiting on an answer for (issue #59).
    """
    if not registry_topic_ids:
        return rows

    def _has_registry_topic(row: Dict[str, Any]) -> bool:
        return not registry_topic_ids.isdisjoint(row.get("topic_ids") or [])

    return sorted(rows, key=lambda r: 0 if _has_registry_topic(r) else 1)


def _registry_topic_ids() -> Set[str]:
    """Topic ids with at least one registered question. Best-effort: an
    unset or malformed registry just disables prioritization rather than
    breaking attribution."""
    from study_scraper.config import get_settings
    from study_scraper.questions import QuestionRegistryError, load_questions

    settings = get_settings()
    try:
        registry = load_questions(settings.questions_yaml_path)
    except (FileNotFoundError, QuestionRegistryError):
        return set()
    return {q.topic_id for q in registry}


def _target_ids(
    storage: PostgresStorage,
    *,
    limit: int,
    study_id: Optional[str],
) -> List[str]:
    if study_id is not None:
        return [study_id]
    rows = storage.query_view(
        "attribution_queue", limit=max(limit, _QUEUE_SCAN_FLOOR)
    )
    rows = prioritize_queue(rows, _registry_topic_ids())
    return [r["id"] for r in rows[:limit]]
