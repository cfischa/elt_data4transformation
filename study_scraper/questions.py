"""Question registry — the standing questions we want answered, tied to topics.

Unifies the two halves of the platform's intent:

    topics.csv    -> WHICH subjects we collect data for (steers the crawl)
    questions.yml -> WHICH propositions we answer within them (this registry)

A registered ``Question`` is a neutral proposition ("Should Germany keep
nuclear power?") scoped to exactly one topic. It is the DECLARATIVE source of a
monitoring ``watch``: ``questions sync`` upserts each into the watches table,
and the existing crawl -> attribute -> digest loop then answers it from ALL
relevant attributions (poll-of-polls over the whole corpus, ``aggregate.py``)
and tracks it over time.

There is deliberately NO new answer path and NO new statistics here: the
registry is the single source, and the already-shipped digest/aggregate
machinery is the single answerer. Loading and watch-spec derivation are pure
functions; only the CLI ``sync``/``answer`` verbs touch the database.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

import yaml
from pydantic import BaseModel, Field, field_validator


class QuestionRegistryError(ValueError):
    """Raised when questions.yml is malformed or inconsistent with topics."""


class Question(BaseModel):
    """One standing proposition, scoped to a single topic."""

    id: str
    topic_id: str
    text: str
    query: str
    since_year: Optional[int] = Field(default=None)

    @field_validator("id", "topic_id", "text", "query")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        value = (value or "").strip()
        if not value:
            raise ValueError("must not be empty")
        return value

    def watch_label(self, topic_name: Optional[str] = None) -> str:
        """Heading used for the digest section when this question is synced
        as a watch. Prefers a human topic name, falls back to the id."""
        head = (topic_name or self.topic_id).strip()
        return f"{head}: {self.text}"


def watch_spec_for(
    question: Question, *, topic_name: Optional[str] = None
) -> Dict[str, object]:
    """Pure mapping from a registry question to the watch upsert arguments.

    Deterministic so ``sync`` is idempotent and unit-testable without a DB:
    the same registry always produces the same ``(query, label, since_year)``.
    """
    return {
        "query": question.query,
        "label": question.watch_label(topic_name),
        "since_year": question.since_year,
    }


def load_questions(
    path: Path, *, valid_topic_ids: Optional[List[str]] = None
) -> List[Question]:
    """Load and validate the question registry.

    Structure (``questions.yml``)::

        version: "0.1.0"
        topics:
          atomkraft:
            - id: atomkraft_keep_or_return
              text: "Should Germany keep or return to nuclear power?"
              query: "nuclear power"
              since_year: 2020        # optional recency floor

    Enforced invariants (statistical + referential integrity):
      - every ``topic_id`` exists in ``valid_topic_ids`` (when provided),
      - question ``id`` is unique across the whole registry,
      - ``query`` is unique across the whole registry (``watches.query`` is
        UNIQUE, so two questions sharing a query would collide on sync).
    """
    if not path.exists():
        raise FileNotFoundError(f"questions registry not found: {path}")

    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:  # pragma: no cover - passthrough
        raise QuestionRegistryError(f"{path}: invalid YAML: {exc}") from exc
    if not isinstance(data, dict):
        raise QuestionRegistryError(f"{path}: top level must be a mapping")

    topics = data.get("topics") or {}
    if not isinstance(topics, dict):
        raise QuestionRegistryError(f"{path}: 'topics' must be a mapping")

    valid = set(valid_topic_ids) if valid_topic_ids is not None else None
    questions: List[Question] = []
    seen_ids: Dict[str, str] = {}
    seen_queries: Dict[str, str] = {}

    for topic_id, items in topics.items():
        topic_id = str(topic_id).strip()
        if valid is not None and topic_id not in valid:
            raise QuestionRegistryError(
                f"{path}: topic {topic_id!r} is not defined in topics.csv"
            )
        if not isinstance(items, list):
            raise QuestionRegistryError(
                f"{path}: topic {topic_id!r} must map to a list of questions"
            )
        for item in items:
            if not isinstance(item, dict):
                raise QuestionRegistryError(
                    f"{path}: question under {topic_id!r} must be a mapping"
                )
            try:
                question = Question(topic_id=topic_id, **item)
            except (TypeError, ValueError) as exc:
                raise QuestionRegistryError(
                    f"{path}: invalid question under {topic_id!r}: {exc}"
                ) from exc

            if question.id in seen_ids:
                raise QuestionRegistryError(
                    f"{path}: duplicate question id {question.id!r} "
                    f"(also under {seen_ids[question.id]!r})"
                )
            qkey = question.query.lower()
            if qkey in seen_queries:
                raise QuestionRegistryError(
                    f"{path}: duplicate query {question.query!r} for "
                    f"{question.id!r} (also {seen_queries[qkey]!r}); "
                    "watches.query is UNIQUE"
                )
            seen_ids[question.id] = topic_id
            seen_queries[qkey] = question.id
            questions.append(question)

    return questions
