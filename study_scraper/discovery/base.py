"""Discovery source protocol + the `Candidate` data class."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, Iterator, List, Optional, Protocol, runtime_checkable

from pydantic import BaseModel, Field

from study_scraper.topics import Topic


class Candidate(BaseModel):
    """Pre-filter candidate emitted by a discovery source."""

    source_id: str
    external_id: str
    canonical_url: str
    title: str
    authors: List[str] = Field(default_factory=list)
    publisher: Optional[str] = None
    publication_date: Optional[date] = None
    language: Optional[str] = None
    abstract: Optional[str] = None
    discovery_query: Optional[str] = None
    raw: Dict[str, Any] = Field(default_factory=dict)


@runtime_checkable
class DiscoverySource(Protocol):
    """Anything that turns a `Topic` into candidates."""

    source_id: str

    def iter_candidates(
        self, topic: Topic, *, limit: Optional[int] = None
    ) -> Iterator[Candidate]:
        ...
