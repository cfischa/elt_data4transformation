"""Domain models.

Per DECISIONS.md A5 a study is any artifact about at least one configured
topic; the boolean `has_quantitative_data` flag distinguishes
data-containing studies from purely qualitative ones without dropping
the latter.

`Study.id` is a deterministic sha256 hex of the canonical URL — so
re-runs against the same source URL upsert the same row, and we never
generate UUIDs we'd then have to dedupe.
"""

from __future__ import annotations

import hashlib
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


def study_id_for(canonical_url: str) -> str:
    """Stable id for a study, derived from its canonical URL."""
    if not canonical_url:
        raise ValueError("canonical_url is required for study id")
    return hashlib.sha256(canonical_url.encode("utf-8")).hexdigest()


class SurveyMetadata(BaseModel):
    """Optional survey-specific metadata extracted from a study artifact.

    All fields are best-effort: scrapers populate what they can find.
    """

    model_config = ConfigDict(extra="allow")

    sample_size: Optional[int] = None
    fieldwork_start: Optional[date] = None
    fieldwork_end: Optional[date] = None
    methodology: Optional[str] = None
    population: Optional[str] = None
    margin_of_error: Optional[float] = None


class Provenance(BaseModel):
    """Where a record came from, for debugging and audit."""

    model_config = ConfigDict(extra="allow")

    discovery_source: str
    discovery_query: Optional[str] = None
    fetch_status_code: Optional[int] = None
    content_type: Optional[str] = None
    content_length: Optional[int] = None
    extractor_version: Optional[str] = None


class Study(BaseModel):
    """A discovered and (at least minimally) extracted study."""

    id: str = Field(min_length=64, max_length=64)
    canonical_url: str
    source_urls: List[str] = Field(default_factory=list)
    title: str
    authors: List[str] = Field(default_factory=list)
    publisher: Optional[str] = None
    publication_date: Optional[date] = None
    language: Optional[str] = None
    topic_ids: List[str] = Field(default_factory=list)
    topic_scores: Dict[str, float] = Field(default_factory=dict)
    has_quantitative_data: bool = False
    abstract: Optional[str] = None
    key_findings: List[str] = Field(default_factory=list)
    survey_metadata: Optional[SurveyMetadata] = None
    raw_artifact_ref: Optional[str] = None
    fetched_at: datetime
    source_id: str
    provenance: Provenance

    @field_validator("canonical_url")
    @classmethod
    def _strip_url(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("canonical_url must be non-empty")
        return stripped

    @field_validator("fetched_at")
    @classmethod
    def _ensure_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    @field_validator("topic_scores")
    @classmethod
    def _scores_in_range(cls, value: Dict[str, float]) -> Dict[str, float]:
        for topic_id, score in value.items():
            if not 0.0 <= score <= 1.0:
                raise ValueError(
                    f"topic_scores[{topic_id!r}] = {score} is outside [0, 1]"
                )
        return value

    @classmethod
    def build(
        cls,
        *,
        canonical_url: str,
        title: str,
        fetched_at: datetime,
        source_id: str,
        provenance: Provenance,
        **extra: Any,
    ) -> "Study":
        """Convenience factory that derives `id` from `canonical_url`."""
        return cls(
            id=study_id_for(canonical_url),
            canonical_url=canonical_url,
            title=title,
            fetched_at=fetched_at,
            source_id=source_id,
            provenance=provenance,
            **extra,
        )


class CrawlRun(BaseModel):
    """Bookkeeping for one CLI invocation against one source + topic."""

    id: str  # UUID hex
    source_id: str
    topic_id: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    candidates_seen: int = 0
    candidates_kept: int = 0
    errors: int = 0
    parameters: Dict[str, Any] = Field(default_factory=dict)
    notes: Optional[str] = None
