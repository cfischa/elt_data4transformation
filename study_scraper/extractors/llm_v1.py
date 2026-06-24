"""llm-v1 — structured attribution extractor (Option A, A21).

Turns a study's text + its regex claims into structured
`(question, position, percentage)` triples via the Anthropic Messages
API. Three design points the maintainer asked for:

  1. **Prompt caching on the system prompt.** The instruction block is
     frozen and marked `cache_control: ephemeral`; the per-study text
     is volatile and goes in `messages` after the cached prefix, so
     every call re-reads the cached system prompt at ~0.1x cost.
  2. **Structured JSON output.** `output_config.format` constrains the
     response to a JSON schema; the parser is a pure function over the
     returned text, so it's testable without the SDK or the network.
  3. **Lazy SDK import.** `import anthropic` happens inside the live
     call only. The pure functions (prompt building, response parsing,
     the schema) import and test with no SDK installed and no API key.

Two run modes mirror the source `from_file` pattern used everywhere
else in this codebase:

  - **Live** (`extract_live`) — calls the API. Needs `ANTHROPIC_API_KEY`.
    This is Option A: metered against the operator's key (model is
    env-overridable for cost — see `MODEL`).
  - **Offline** (`parse_response`) — feed a model response captured
    elsewhere (e.g. a Cowork session) back through the same parser.
    This is the zero-extra-API-cost path and the test path.

Model: defaults to `claude-opus-4-8` (the skill default). Override with
`STUDY_SCRAPER_LLM_MODEL` for cheaper bulk runs (e.g. claude-haiku-4-5)
— that's the operator's cost decision, documented in the RUNBOOK.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


EXTRACTOR_ID = "llm-v1"
MODEL = os.environ.get("STUDY_SCRAPER_LLM_MODEL", "claude-opus-4-8")
MAX_TOKENS = 4096

_VALID_POSITIONS = ("support", "oppose", "neutral", "unspecified")

# Frozen instruction block — the cacheable prefix. Keep this byte-stable:
# any edit invalidates the prompt cache for every study processed after.
SYSTEM_PROMPT = """\
You extract structured public-opinion findings from German political
study text. You are given a study's title, abstract, and a list of
numeric claims already located by a regex pass (each with the
surrounding sentence). Your job: for each finding that expresses how a
population feels about a concrete question, emit one structured
attribution.

For each attribution provide:
- question: the survey question or proposition, phrased neutrally as a
  short statement (e.g. "Germany should re-enter nuclear energy").
  Translate to concise English; keep proper nouns.
- position: one of support | oppose | neutral | unspecified. Use the
  stance the percentage describes (e.g. "55% befuerworten" -> support;
  "36% lehnen ab" -> oppose; "9% unentschieden" -> neutral). Use
  unspecified when the number is a methodology figure (sample size,
  field dates) or its stance is unclear.
- percentage: the number as 0..100, or null if the finding has no
  percentage.
- population: who was asked, if stated (e.g. "Wahlberechtigte ab 18"),
  else null.
- confidence: your confidence 0..1 that this attribution is faithful
  to the text.

Rules:
- Only emit attributions grounded in the provided text. Do not invent
  figures or infer beyond what is stated.
- One attribution per distinct (question, position) the text reports.
- Skip pure methodology numbers unless they are the only signal, in
  which case use position=unspecified.
- Return an empty list if the text contains no opinion findings.
"""

# JSON schema for output_config.format. Note the API's structured-output
# limits: no minLength/maxLength, no numeric min/max — so percentage is a
# plain number and we range-check in the parser.
OUTPUT_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "attributions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "question": {"type": "string"},
                    "position": {
                        "type": "string",
                        "enum": list(_VALID_POSITIONS),
                    },
                    "percentage": {"type": ["number", "null"]},
                    "population": {"type": ["string", "null"]},
                    "confidence": {"type": "number"},
                },
                "required": [
                    "question", "position", "percentage",
                    "population", "confidence",
                ],
                "additionalProperties": False,
            },
        },
    },
    "required": ["attributions"],
    "additionalProperties": False,
}


@dataclass
class Attribution:
    study_id: str
    question: str
    position: str
    percentage: Optional[float]
    population: Optional[str]
    confidence: Optional[float]
    model: str = EXTRACTOR_ID
    raw: Dict[str, Any] = field(default_factory=dict)

    @property
    def id(self) -> str:
        pct = "" if self.percentage is None else f"{self.percentage:g}"
        key = f"{self.study_id}|{self.question}|{self.position}|{pct}|{self.model}"
        return hashlib.sha256(key.encode("utf-8")).hexdigest()


# ----------------------------------------------------------------------
# Pure functions — no SDK, no network, fully unit-testable
# ----------------------------------------------------------------------


def build_user_prompt(
    *,
    title: Optional[str],
    abstract: Optional[str],
    claim_snippets: List[str],
) -> str:
    """Assemble the volatile per-study prompt (goes AFTER the cached
    system prompt, so it never participates in the cache prefix)."""
    parts: List[str] = []
    parts.append(f"TITLE: {title or '(none)'}")
    parts.append(f"\nABSTRACT:\n{abstract or '(none)'}")
    if claim_snippets:
        parts.append("\nREGEX CLAIMS (sentence context):")
        for i, snip in enumerate(claim_snippets, start=1):
            parts.append(f"{i}. {snip}")
    parts.append(
        "\nExtract structured attributions as instructed. "
        "Return JSON matching the schema."
    )
    return "\n".join(parts)


def parse_response(text: str, *, study_id: str, model: str = EXTRACTOR_ID) -> List[Attribution]:
    """Parse the model's JSON response into Attributions.

    Tolerant of code-fenced JSON. Skips malformed items rather than
    failing the whole batch. Range-checks percentage (0..100) and
    normalizes the position enum.
    """
    payload = _loads_lenient(text)
    if not isinstance(payload, dict):
        return []
    items = payload.get("attributions")
    if not isinstance(items, list):
        return []

    out: List[Attribution] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        question = (item.get("question") or "").strip()
        if not question:
            continue
        position = str(item.get("position") or "unspecified").strip().lower()
        if position not in _VALID_POSITIONS:
            position = "unspecified"
        percentage = _coerce_percentage(item.get("percentage"))
        population = item.get("population")
        if isinstance(population, str):
            population = population.strip() or None
        else:
            population = None
        confidence = _coerce_unit(item.get("confidence"))
        out.append(
            Attribution(
                study_id=study_id,
                question=question,
                position=position,
                percentage=percentage,
                population=population,
                confidence=confidence,
                model=model,
                raw=item,
            )
        )
    return out


def _loads_lenient(text: str) -> Any:
    text = (text or "").strip()
    if not text:
        return None
    if text.startswith("```"):
        # strip a ```json ... ``` fence
        inner = text.split("```", 2)
        if len(inner) >= 2:
            body = inner[1]
            if body.lstrip().lower().startswith("json"):
                body = body.lstrip()[4:]
            text = body.strip()
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        # last resort: find the outermost object
        start = text.find("{")
        end = text.rfind("}")
        if 0 <= start < end:
            try:
                return json.loads(text[start:end + 1])
            except (ValueError, TypeError):
                return None
    return None


def _coerce_percentage(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        f = float(value)
    except (ValueError, TypeError):
        return None
    if 0.0 <= f <= 100.0:
        return f
    return None


def _coerce_unit(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        f = float(value)
    except (ValueError, TypeError):
        return None
    return min(1.0, max(0.0, f))


# ----------------------------------------------------------------------
# Live call — lazy SDK import; Option A (metered)
# ----------------------------------------------------------------------


def extract_live(
    *,
    study_id: str,
    title: Optional[str],
    abstract: Optional[str],
    claim_snippets: List[str],
    model: Optional[str] = None,
    client: Any = None,
) -> List[Attribution]:
    """Call the Anthropic Messages API for one study. Returns
    Attributions. Requires `ANTHROPIC_API_KEY` (or a pre-built client).

    Prompt caching: the system prompt is sent as a cache_control
    ephemeral block; the per-study user prompt follows it. Across many
    studies the system prefix is served from cache.
    """
    model = model or MODEL
    if client is None:
        import anthropic  # lazy: tests + offline path never import this
        client = anthropic.Anthropic()

    response = client.messages.create(
        model=model,
        max_tokens=MAX_TOKENS,
        system=[
            {
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        output_config={"format": {"type": "json_schema", "schema": OUTPUT_SCHEMA}},
        messages=[
            {
                "role": "user",
                "content": build_user_prompt(
                    title=title, abstract=abstract,
                    claim_snippets=claim_snippets,
                ),
            }
        ],
    )
    # output_config.format guarantees the first text block is valid JSON.
    text = next(
        (b.text for b in response.content if getattr(b, "type", None) == "text"),
        "",
    )
    # Record the model id that actually served the response.
    served_model = getattr(response, "model", model)
    return parse_response(text, study_id=study_id, model=served_model)
