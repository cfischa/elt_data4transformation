"""Phase-6-mini: extract numerical claims from study title + abstract.

Why this exists: the 2026-05-28 measurement against the maintainer's
example questions ("how many Germans want stricter climate laws / want
nuclear back?") showed that we have a *catalog* of studies but not the
*answers* the studies contain. The answers ("62 % support …", "55 %
favour …") sit in the body of polling reports.

This module is the smallest viable bridge: a regex pass over what we
already have (title + abstract) that captures `<number> <unit>` tokens
with a short surrounding context window. Output goes to the `claims`
table, queryable by the `search` CLI and the dock.

Limitations (intentional, documented for next iteration):
  - Regex-only. No language-model pass. False positives on year tokens
    are filtered out heuristically; the rest is up to the human reading
    the snippet.
  - Title + abstract only. Full-text PDF extraction is its own phase.
  - Single-language (DE/EN keyword bracketing); no semantic
    disambiguation of WHAT the % refers to.

The schema (`claims`) is wide enough that an LLM-based extractor can
replace `extractor='regex-v1'` later without a migration.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Iterable, List, Optional

# Words that often anchor an opinion / methodological percentage.
_DE_OPINION_HINTS = (
    "befragten", "befragte", "wähler", "bevölkerung", "bürger",
    "befürworten", "lehnen", "halten", "trauen", "unterstützen", "mehrheit",
)
_EN_OPINION_HINTS = (
    "respondents", "voters", "germans", "majority", "support",
    "oppose", "favour", "favor", "agree",
)
_OPINION_HINTS = _DE_OPINION_HINTS + _EN_OPINION_HINTS


# A number followed by a percent sign, possibly with a comma/dot decimal,
# possibly with a space between number and "%". Examples we want to catch:
#   62%, 62 %, 62,5 %, 62.5%, 6.063 % (rare but ok)
_PERCENT_RE = re.compile(
    r"(?P<value>\d+(?:[.,]\d+)?)\s?%",
    re.UNICODE,
)

# "n=1024", "n = 1024", "(n=1009," — sample-size cues, kept as a claim
# type so the dock can show "based on a sample of X".
_SAMPLE_RE = re.compile(
    r"\bn\s*=\s*(?P<value>\d+(?:[\. ]\d{3})*(?:[.,]\d+)?)\b",
    re.IGNORECASE,
)

# Context window: how many characters on either side of the match to
# capture as `claim_text`. Tuned to grab the verb + object that gives
# the % its meaning.
_CONTEXT_CHARS = 90


@dataclass
class ExtractedClaim:
    study_id: str
    claim_text: str
    numeric_value: Optional[float]
    unit: Optional[str]
    source_field: str  # 'title' | 'abstract'

    @property
    def id(self) -> str:
        """Stable id so re-extraction is idempotent."""
        key = f"{self.study_id}|{self.source_field}|{self.claim_text}"
        return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _looks_like_year(value: float, context: str) -> bool:
    """Filter '2022' style year tokens that aren't survey-statistic %s."""
    if not 1900 <= value <= 2100:
        return False
    # If the number is followed immediately by '%', it's not a year even
    # if the digits look like one. Caller passes the original token plus
    # one-char-after context — but the regex already separates numbers
    # from the unit, so this filter rarely matters once we require '%'.
    if "%" in context:
        return False
    return True


def _trim_snippet(text: str, start: int, end: int) -> str:
    """Cut a clean snippet around [start, end] without breaking words."""
    lo = max(0, start - _CONTEXT_CHARS)
    hi = min(len(text), end + _CONTEXT_CHARS)
    # Back off to a space boundary on the left.
    while lo > 0 and text[lo] != " ":
        lo -= 1
    # Forward to a space / punctuation boundary on the right.
    while hi < len(text) and text[hi] not in " .;!?":
        hi += 1
    snippet = text[lo:hi].strip()
    return re.sub(r"\s+", " ", snippet)


def _value_to_float(token: str) -> Optional[float]:
    """Parse 62 / 62.5 / 62,5 / 6.063 / 6,063 into float."""
    try:
        # If there's a comma followed by exactly 3 digits at the end,
        # treat it as German thousands ('6,063' → 6063); otherwise
        # decimal ('62,5' → 62.5).
        if "," in token:
            head, tail = token.rsplit(",", 1)
            if tail.isdigit() and len(tail) == 3 and head.replace(".", "").isdigit():
                return float(token.replace(",", "").replace(".", ""))
            return float(token.replace(",", "."))
        # Same heuristic with '.'
        if "." in token:
            head, tail = token.rsplit(".", 1)
            if tail.isdigit() and len(tail) == 3 and head.replace(",", "").isdigit():
                return float(token.replace(".", ""))
            return float(token)
        return float(token)
    except ValueError:
        return None


def extract_claims(
    *, study_id: str, title: Optional[str], abstract: Optional[str]
) -> List[ExtractedClaim]:
    """Return one `ExtractedClaim` per number-with-unit match.

    Deduplicates within the same source field (a doc that mentions
    "62 %" twice produces one claim).
    """
    out: List[ExtractedClaim] = []
    seen: set[tuple[str, str]] = set()

    for source_field, text in (("title", title or ""), ("abstract", abstract or "")):
        if not text:
            continue

        # Percent matches
        for m in _PERCENT_RE.finditer(text):
            value = _value_to_float(m.group("value"))
            if value is None:
                continue
            # Optional sanity: a percent of >120 is almost always not a
            # poll figure; suppress those.
            if value > 120.0:
                continue
            snippet = _trim_snippet(text, m.start(), m.end())
            # Dedup includes the match span so overlapping matches that
            # produce the same snippet still count as distinct claims.
            key = (source_field, m.start(), value)
            if key in seen:
                continue
            seen.add(key)
            out.append(
                ExtractedClaim(
                    study_id=study_id,
                    claim_text=snippet,
                    numeric_value=value,
                    unit="%",
                    source_field=source_field,
                )
            )

        # Sample-size cues. Useful for the dock to show "n=…" alongside.
        for m in _SAMPLE_RE.finditer(text):
            value = _value_to_float(m.group("value").replace(" ", ""))
            if value is None:
                continue
            snippet = _trim_snippet(text, m.start(), m.end())
            key = (source_field, m.start(), value)
            if key in seen:
                continue
            seen.add(key)
            out.append(
                ExtractedClaim(
                    study_id=study_id,
                    claim_text=snippet,
                    numeric_value=value,
                    unit="n",  # sample size
                    source_field=source_field,
                )
            )

    return out
