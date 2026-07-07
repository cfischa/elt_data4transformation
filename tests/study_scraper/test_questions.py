"""Question registry: loader validation, watch-spec derivation, CLI, and a
consistency guard on the shipped config/topics/questions.yml.

The registry is the declarative source of the monitoring watches — the piece
that makes "topics and questions are ONE thing" real. These tests keep it
honest offline; the sync/answer DB paths are DSN-gated integration tests.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import pytest
from typer.testing import CliRunner

from study_scraper.cli import app
from study_scraper.config import get_settings
from study_scraper.questions import (
    Question,
    QuestionRegistryError,
    load_questions,
    watch_spec_for,
)
from study_scraper.topics import load_topics


TEST_DSN = os.environ.get("STUDY_SCRAPER_TEST_DSN")


def _plain(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def _write(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "questions.yml"
    p.write_text(body, encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# Loader — happy path
# ---------------------------------------------------------------------------

def test_load_questions_happy_path(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
version: "0.1.0"
topics:
  atomkraft:
    - id: atom_keep
      text: "Keep nuclear power?"
      query: "nuclear power"
      since_year: 2020
  klima:
    - id: klima_policy
      text: "Stronger climate policy?"
      query: "climate law"
""",
    )
    qs = load_questions(path, valid_topic_ids=["atomkraft", "klima"])
    assert [q.id for q in qs] == ["atom_keep", "klima_policy"]
    assert qs[0].topic_id == "atomkraft"
    assert qs[0].since_year == 2020
    assert qs[1].since_year is None


def test_load_questions_no_topic_validation(tmp_path: Path) -> None:
    """When valid_topic_ids is omitted, any topic id is accepted."""
    path = _write(
        tmp_path,
        """
topics:
  whatever:
    - id: q1
      text: "T"
      query: "q"
""",
    )
    qs = load_questions(path)  # no FK check
    assert qs[0].topic_id == "whatever"


# ---------------------------------------------------------------------------
# Loader — validation failures
# ---------------------------------------------------------------------------

def test_unknown_topic_id_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
topics:
  not_a_topic:
    - id: q1
      text: "T"
      query: "q"
""",
    )
    with pytest.raises(QuestionRegistryError, match="not defined in topics.csv"):
        load_questions(path, valid_topic_ids=["atomkraft"])


def test_duplicate_question_id_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
topics:
  atomkraft:
    - id: dup
      text: "A"
      query: "a"
  klima:
    - id: dup
      text: "B"
      query: "b"
""",
    )
    with pytest.raises(QuestionRegistryError, match="duplicate question id"):
        load_questions(path, valid_topic_ids=["atomkraft", "klima"])


def test_duplicate_query_rejected(tmp_path: Path) -> None:
    """watches.query is UNIQUE — two questions sharing a query would collide."""
    path = _write(
        tmp_path,
        """
topics:
  atomkraft:
    - id: q1
      text: "A"
      query: "Nuclear Power"
  klima:
    - id: q2
      text: "B"
      query: "nuclear power"
""",
    )
    with pytest.raises(QuestionRegistryError, match="duplicate query"):
        load_questions(path, valid_topic_ids=["atomkraft", "klima"])


def test_empty_field_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
topics:
  atomkraft:
    - id: q1
      text: "  "
      query: "a"
""",
    )
    with pytest.raises(QuestionRegistryError):
        load_questions(path, valid_topic_ids=["atomkraft"])


def test_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_questions(tmp_path / "nope.yml")


def test_topic_not_a_list_rejected(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        """
topics:
  atomkraft:
    id: q1
""",
    )
    with pytest.raises(QuestionRegistryError, match="list of questions"):
        load_questions(path, valid_topic_ids=["atomkraft"])


# ---------------------------------------------------------------------------
# watch_spec_for — pure, deterministic
# ---------------------------------------------------------------------------

def test_watch_spec_deterministic_and_labeled() -> None:
    q = Question(
        id="q1", topic_id="atomkraft",
        text="Keep nuclear power?", query="nuclear power", since_year=2020,
    )
    spec1 = watch_spec_for(q, topic_name="Atomkraft")
    spec2 = watch_spec_for(q, topic_name="Atomkraft")
    assert spec1 == spec2  # deterministic → idempotent sync
    assert spec1["query"] == "nuclear power"
    assert spec1["label"] == "Atomkraft: Keep nuclear power?"
    assert spec1["since_year"] == 2020


def test_watch_label_falls_back_to_topic_id() -> None:
    q = Question(id="q1", topic_id="atomkraft", text="X?", query="x")
    assert q.watch_label() == "atomkraft: X?"


# ---------------------------------------------------------------------------
# The shipped registry must be consistent with the shipped topics
# ---------------------------------------------------------------------------

def test_shipped_registry_is_valid() -> None:
    settings = get_settings()
    topic_ids = [t.id for t in load_topics(settings.topics_csv_path)]
    qs = load_questions(settings.questions_yaml_path, valid_topic_ids=topic_ids)
    assert qs, "shipped questions.yml should not be empty"
    # ids + queries unique is enforced by the loader; assert FK coverage too.
    assert all(q.topic_id in topic_ids for q in qs)


# ---------------------------------------------------------------------------
# CLI — registration + offline list over the shipped registry
# ---------------------------------------------------------------------------

def test_cli_questions_registered() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["questions", "--help"])
    assert result.exit_code == 0
    out = _plain(result.output)
    assert "sync" in out and "answer" in out and "list" in out


def test_cli_questions_list_offline() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["questions", "list"])
    assert result.exit_code == 0
    out = _plain(result.output)
    assert "atomkraft" in out
    assert "nuclear power" in out


def test_cli_questions_list_filter_topic() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["questions", "list", "--topic", "verteidigung"])
    assert result.exit_code == 0
    out = _plain(result.output)
    assert "verteidigung" in out
    assert "atomkraft" not in out


# ---------------------------------------------------------------------------
# Integration (real Postgres) — sync idempotency
# ---------------------------------------------------------------------------

pytestmark_integration = pytest.mark.skipif(
    not TEST_DSN, reason="STUDY_SCRAPER_TEST_DSN not set; skipping integration"
)


@pytest.fixture(scope="module")
def storage():
    from study_scraper.storage import PostgresStorage
    assert TEST_DSN is not None
    store = PostgresStorage(TEST_DSN)
    store.migrate()
    return store


@pytestmark_integration
def test_sync_is_idempotent(storage) -> None:
    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE study_scraper.watch_snapshots CASCADE")
            cur.execute("TRUNCATE study_scraper.watches CASCADE")
        conn.commit()

    settings = get_settings()
    topic_ids = [t.id for t in load_topics(settings.topics_csv_path)]
    registry = load_questions(
        settings.questions_yaml_path, valid_topic_ids=topic_ids
    )

    def _sync() -> None:
        for q in registry:
            spec = watch_spec_for(q)
            storage.add_watch(
                query=str(spec["query"]),
                label=str(spec["label"]),
                since_year=spec["since_year"],
            )

    _sync()
    first = storage.list_watches()
    _sync()  # re-sync must not duplicate
    second = storage.list_watches()

    assert len(first) == len(registry)
    assert len(second) == len(registry)
    assert {w["query"] for w in first} == {w["query"] for w in second}
