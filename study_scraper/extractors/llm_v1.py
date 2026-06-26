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
import re
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
- position: one of support | oppose | neutral | unspecified. Map the
  German stance verb the percentage describes:
    * SUPPORT  : befuerworten, unterstuetzen, sind dafuer, stimmen zu,
                 sprechen sich fuer ... aus, halten ... fuer
                 richtig/noetig, trauen ... zu, finden gut
    * OPPOSE   : lehnen ab, sind dagegen, sprechen sich gegen ... aus,
                 halten ... fuer falsch, misstrauen, kritisieren
    * NEUTRAL  : unentschieden, weder noch, teils teils, neutral,
                 keine Meinung, egal
    * UNSPECIFIED: methodology figures (sample size, field dates,
                 margins) or when the stance is genuinely unclear.
- percentage: the number as 0..100, or null if the finding has no
  percentage.
- population: who was asked, if stated (e.g. "Wahlberechtigte ab 18"),
  else null.
- confidence: your confidence 0..1 that this attribution is faithful
  to the text.
- source_span: a SHORT VERBATIM quote (<=200 chars) copied EXACTLY,
  character-for-character, from the provided title/abstract/claims that
  contains this finding's number and stance. Copy the original German;
  do NOT paraphrase or translate the source_span. It is the evidence
  for the finding and will be checked against the source text.

Rules:
- Only emit attributions grounded in the provided text. Do not invent
  figures or infer beyond what is stated. Every source_span must appear
  verbatim in the input.
- One attribution per distinct (question, position) the text reports.
- For a single question, support + oppose + neutral percentages should
  not exceed ~100%. Never fabricate complementary figures.
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
                    "source_span": {"type": "string"},
                },
                "required": [
                    "question", "position", "percentage",
                    "population", "confidence", "source_span",
                ],
                "additionalProperties": False,
            },
        },
    },
    "required": ["attributions"],
    "additionalProperties": False,
}


# German stance verbs → position, applied defensively in the parser when
# the model returns a German word instead of the enum (F3). The prompt
# already teaches these; this is a safety net, not the primary path.
_STANCE_MAP = {
    "support": "support", "oppose": "oppose",
    "neutral": "neutral", "unspecified": "unspecified",
    "befuerworten": "support", "befürworten": "support",
    "unterstuetzen": "support", "unterstützen": "support",
    "dafuer": "support", "dafür": "support", "zustimmung": "support",
    "ablehnen": "oppose", "lehnen ab": "oppose", "dagegen": "oppose",
    "ablehnung": "oppose", "gegen": "oppose",
    "unentschieden": "neutral", "neutral_de": "neutral",
    "weiss nicht": "neutral", "weiß nicht": "neutral",
}


@dataclass
class Attribution:
    study_id: Optional[str] = None
    question: str = ""
    position: str = "unspecified"
    percentage: Optional[float] = None
    population: Optional[str] = None
    confidence: Optional[float] = None
    model: str = EXTRACTOR_ID
    raw: Dict[str, Any] = field(default_factory=dict)
    source_span: Optional[str] = None
    # Exactly one target: study (academic) or source_record (lake).
    source_record_id: Optional[str] = None
    # None = not checked (no source text available); True/False = checked
    # against the source text (F1 hallucination guard).
    grounded: Optional[bool] = None

    @property
    def target_id(self) -> Optional[str]:
        return self.source_record_id or self.study_id

    @property
    def id(self) -> str:
        pct = "" if self.percentage is None else f"{self.percentage:g}"
        key = f"{self.target_id}|{self.question}|{self.position}|{pct}|{self.model}"
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


def _normalize_for_grounding(s: str) -> str:
    """Whitespace-insensitive, case-insensitive form for substring checks."""
    return re.sub(r"\s+", " ", (s or "").lower()).strip()


def _coerce_position(value: Any) -> str:
    """Map the model's position to the enum, with a German-verb safety net."""
    raw = str(value or "unspecified").strip().lower()
    if raw in _VALID_POSITIONS:
        return raw
    return _STANCE_MAP.get(raw, "unspecified")


def parse_response(
    text: str,
    *,
    study_id: Optional[str] = None,
    source_record_id: Optional[str] = None,
    model: str = EXTRACTOR_ID,
    source_text: Optional[str] = None,
) -> List[Attribution]:
    """Parse the model's JSON response into Attributions.

    Tolerant of code-fenced JSON. Skips malformed items rather than
    failing the whole batch. Range-checks percentage (0..100) and
    normalizes the position enum.

    Accuracy guards:
      - F1 grounding: if `source_text` is given, each item's `source_span`
        is checked to appear verbatim (whitespace/case-insensitive) in it.
        Ungrounded findings (a likely hallucination) are flagged
        `grounded=False` and their confidence is capped at 0.3 so they
        sort last and can be filtered. When `source_text` is None,
        grounding is left unchecked (`grounded=None`).
      - F5 distribution: per question, if support+oppose+neutral exceed
        ~120% the items are tagged `raw.distribution_check=False`.
    """
    payload = _loads_lenient(text)
    if not isinstance(payload, dict):
        return []
    items = payload.get("attributions")
    if not isinstance(items, list):
        return []

    src_norm = _normalize_for_grounding(source_text) if source_text else None

    out: List[Attribution] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        question = (item.get("question") or "").strip()
        if not question:
            continue
        position = _coerce_position(item.get("position"))
        percentage = _coerce_percentage(item.get("percentage"))
        population = item.get("population")
        if isinstance(population, str):
            population = population.strip() or None
        else:
            population = None
        confidence = _coerce_unit(item.get("confidence"))

        span = item.get("source_span")
        span = span.strip() if isinstance(span, str) else None

        grounded: Optional[bool] = None
        if src_norm is not None and span:
            grounded = _normalize_for_grounding(span) in src_norm
            if not grounded:
                # Likely hallucination: keep for audit but cap confidence.
                confidence = min(confidence if confidence is not None else 0.3, 0.3)

        raw = dict(item)
        raw["grounded"] = grounded
        out.append(
            Attribution(
                study_id=study_id,
                source_record_id=source_record_id,
                question=question,
                position=position,
                percentage=percentage,
                population=population,
                confidence=confidence,
                model=model,
                raw=raw,
                source_span=span,
                grounded=grounded,
            )
        )

    _flag_distribution(out)
    return out


def _flag_distribution(attrs: List[Attribution]) -> None:
    """F5: per question, if support+oppose+neutral percentages exceed ~120%,
    tag every item of that question raw.distribution_check=False (audit-only,
    non-destructive)."""
    by_q: Dict[str, List[Attribution]] = {}
    for a in attrs:
        by_q.setdefault(_normalize_for_grounding(a.question), []).append(a)
    for group in by_q.values():
        total = sum(
            a.percentage for a in group
            if a.percentage is not None
            and a.position in ("support", "oppose", "neutral")
        )
        ok = total <= 120.0
        for a in group:
            a.raw["distribution_check"] = ok


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
    # Source text for F1 grounding: exactly what the model was shown.
    source_text = build_user_prompt(
        title=title, abstract=abstract, claim_snippets=claim_snippets,
    )
    return parse_response(
        text, study_id=study_id, model=served_model, source_text=source_text,
    )
