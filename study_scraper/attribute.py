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
from typing import Any, Dict, List, Optional

from study_scraper.extractors import llm_v1
from study_scraper.storage import PostgresStorage


LOGGER = logging.getLogger(__name__)


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
        attrs = llm_v1.parse_response(text, study_id=sid, model=model)
        n = storage.upsert_attributions(sid, attrs, model=model)
        results.append({"study_id": sid, "status": "ok", "attributions": n})
    return results


def _target_ids(
    storage: PostgresStorage,
    *,
    limit: int,
    study_id: Optional[str],
) -> List[str]:
    if study_id is not None:
        return [study_id]
    rows = storage.query_view("attribution_queue", limit=limit)
    return [r["id"] for r in rows]
