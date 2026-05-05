"""Topic model + CSV loader.

Per DECISIONS.md A4 the topics sheet is `config/topics/topics.csv`:

    id, name, description, language, include_keywords, exclude_keywords, synonyms

List-valued columns are pipe-separated (`|`). Multiple rows per topic id
are allowed — one row per language. The loader groups rows by `id` and
returns one `Topic` object per id with per-language keyword sets.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Iterable, List, Mapping

from pydantic import BaseModel, Field, field_validator


_LIST_SEPARATOR = "|"
_REQUIRED_COLUMNS = (
    "id",
    "name",
    "description",
    "language",
    "include_keywords",
    "exclude_keywords",
    "synonyms",
)


class TopicLocale(BaseModel):
    """Per-language matching surface for a topic."""

    name: str
    description: str
    include_keywords: List[str] = Field(default_factory=list)
    exclude_keywords: List[str] = Field(default_factory=list)
    synonyms: List[str] = Field(default_factory=list)

    @field_validator("include_keywords", "exclude_keywords", "synonyms", mode="before")
    @classmethod
    def _split_pipe_separated(cls, value: object) -> List[str]:
        if value is None or value == "":
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str):
            return [part.strip() for part in value.split(_LIST_SEPARATOR) if part.strip()]
        raise TypeError(f"unexpected type for keyword column: {type(value)!r}")


class Topic(BaseModel):
    """A topic, possibly with multiple language locales."""

    id: str
    locales: Dict[str, TopicLocale]

    @property
    def primary_name(self) -> str:
        if "de" in self.locales:
            return self.locales["de"].name
        return next(iter(self.locales.values())).name

    def all_keywords(self) -> List[str]:
        """All include + synonym terms across locales, lowercased."""
        out: List[str] = []
        for locale in self.locales.values():
            out.extend(term.lower() for term in locale.include_keywords)
            out.extend(term.lower() for term in locale.synonyms)
        # de-duplicate, preserve order
        seen: set[str] = set()
        unique: List[str] = []
        for term in out:
            if term not in seen:
                seen.add(term)
                unique.append(term)
        return unique


class TopicCSVError(ValueError):
    """Raised when the topics CSV is malformed."""


def load_topics(path: Path) -> List[Topic]:
    """Load topics from a CSV file; group rows by `id`, one locale per row."""
    if not path.exists():
        raise FileNotFoundError(f"topics CSV not found: {path}")

    rows = _read_rows(path)
    by_id: Dict[str, Dict[str, TopicLocale]] = {}
    order: List[str] = []

    for line_no, row in enumerate(rows, start=2):  # +1 header, +1 1-based
        topic_id = row["id"].strip()
        language = row["language"].strip().lower()
        if not topic_id:
            raise TopicCSVError(f"line {line_no}: empty topic id")
        if not language:
            raise TopicCSVError(f"line {line_no}: empty language for topic {topic_id!r}")
        if topic_id not in by_id:
            by_id[topic_id] = {}
            order.append(topic_id)
        if language in by_id[topic_id]:
            raise TopicCSVError(
                f"line {line_no}: duplicate locale {language!r} for topic {topic_id!r}"
            )
        by_id[topic_id][language] = TopicLocale(
            name=row["name"].strip(),
            description=row["description"].strip(),
            include_keywords=row["include_keywords"],
            exclude_keywords=row["exclude_keywords"],
            synonyms=row["synonyms"],
        )

    return [Topic(id=topic_id, locales=by_id[topic_id]) for topic_id in order]


def _read_rows(path: Path) -> Iterable[Mapping[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise TopicCSVError(f"{path} has no header row")
        missing = [col for col in _REQUIRED_COLUMNS if col not in reader.fieldnames]
        if missing:
            raise TopicCSVError(
                f"{path} is missing required columns: {', '.join(missing)}"
            )
        return list(reader)
