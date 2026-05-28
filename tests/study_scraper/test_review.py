"""Tests for Q12: the pending / kept / rejected review queue.

Real-Postgres integration tests; skipped without STUDY_SCRAPER_TEST_DSN.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator

import pytest
from typer.testing import CliRunner

from study_scraper.cli import app
from study_scraper.discovery.ssoar import SSOARSource
from study_scraper.pipeline import run_one
from study_scraper.storage import PostgresStorage
from study_scraper.topics import load_topics


TEST_DSN = os.environ.get("STUDY_SCRAPER_TEST_DSN")
FIXTURE = (
    Path(__file__).resolve().parent / "fixtures" / "ssoar" / "klima_records.xml"
)

pytestmark = pytest.mark.skipif(
    not TEST_DSN, reason="STUDY_SCRAPER_TEST_DSN not set; skipping review tests"
)


@pytest.fixture(scope="module")
def storage() -> PostgresStorage:
    assert TEST_DSN is not None
    store = PostgresStorage(TEST_DSN)
    store.migrate()
    return store


@pytest.fixture(autouse=True)
def _clean_tables(storage: PostgresStorage) -> Iterator[None]:
    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE study_scraper.crawl_run_studies CASCADE")
            cur.execute("TRUNCATE study_scraper.studies CASCADE")
            cur.execute("TRUNCATE study_scraper.crawl_runs CASCADE")
        conn.commit()
    yield


@pytest.fixture(scope="module")
def klima_topic():
    topics = load_topics(
        Path(__file__).resolve().parents[2] / "config" / "topics" / "topics.csv"
    )
    return next(t for t in topics if t.id == "klima")


# --------------------------------------------------------------------------
# Pipeline writes the right statuses
# --------------------------------------------------------------------------


def test_default_threshold_keeps_six_no_pending(
    storage: PostgresStorage, klima_topic
) -> None:
    """At the default min_score=0.2 the klima fixture has 6 kept, 0 pending."""
    with SSOARSource(from_file=FIXTURE) as src:
        run_one(source=src, topic=klima_topic, storage=storage, min_score=0.2)
    assert len(storage.list_studies(topic_id="klima", status="kept")) == 6
    assert storage.list_studies(topic_id="klima", status="pending") == []


def test_raised_threshold_demotes_to_pending(
    storage: PostgresStorage, klima_topic
) -> None:
    """At min_score=0.4 only the strong hits stay kept; rest go to pending."""
    with SSOARSource(from_file=FIXTURE) as src:
        run_one(source=src, topic=klima_topic, storage=storage, min_score=0.4)
    kept = storage.list_studies(topic_id="klima", status="kept", limit=50)
    pending = storage.list_studies(topic_id="klima", status="pending", limit=50)
    # Erdgas record scored 0.5 (3 includes) — it should be kept.
    assert any("Erdgas" in r["title"] for r in kept)
    # The two Energiewende records scored 0.2 — they should be pending.
    assert any("Energiewende" in r["title"] for r in pending)
    # Everything kept has score >= 0.4
    for r in kept:
        assert r["topic_scores"]["klima"] >= 0.4
    # Everything pending has 0 < score < 0.4
    for r in pending:
        assert 0.0 < r["topic_scores"]["klima"] < 0.4


def test_negative_control_still_dropped_entirely(
    storage: PostgresStorage, klima_topic
) -> None:
    """score == 0 means not stored at all (not even as pending)."""
    with SSOARSource(from_file=FIXTURE) as src:
        run_one(source=src, topic=klima_topic, storage=storage, min_score=0.0001)
    all_rows = storage.list_studies(status=None, limit=100)
    assert not any("Datenmigration" in r["title"] for r in all_rows)


# --------------------------------------------------------------------------
# Promote / reject + stickiness across re-runs
# --------------------------------------------------------------------------


def test_promote_moves_pending_to_kept(
    storage: PostgresStorage, klima_topic
) -> None:
    with SSOARSource(from_file=FIXTURE) as src:
        run_one(source=src, topic=klima_topic, storage=storage, min_score=0.4)
    pending = storage.list_studies(topic_id="klima", status="pending", limit=1)
    pid = pending[0]["id"]

    assert storage.promote_study(pid, reviewed_by="alice") is True
    row = storage.get_study(pid)
    assert row["status"] == "kept"
    assert row["reviewed_by"] == "alice"
    assert row["reviewed_at"] is not None


def test_reject_records_reason_and_reviewer(
    storage: PostgresStorage, klima_topic
) -> None:
    with SSOARSource(from_file=FIXTURE) as src:
        run_one(source=src, topic=klima_topic, storage=storage, min_score=0.4)
    pending = storage.list_studies(topic_id="klima", status="pending", limit=1)
    pid = pending[0]["id"]

    assert storage.reject_study(pid, reviewed_by="bob", reason="off-topic") is True
    row = storage.get_study(pid)
    assert row["status"] == "rejected"
    assert row["reviewed_by"] == "bob"
    assert row["rejected_reason"] == "off-topic"


def test_rejected_status_is_sticky_across_reruns(
    storage: PostgresStorage, klima_topic
) -> None:
    """A rejected candidate stays rejected even if a later run would
    have classified it as kept."""
    # First run at low threshold → everything that survives excludes
    # is kept.
    with SSOARSource(from_file=FIXTURE) as src:
        run_one(source=src, topic=klima_topic, storage=storage, min_score=0.0001)
    # Reject one of them.
    kept = storage.list_studies(topic_id="klima", status="kept", limit=1)
    pid = kept[0]["id"]
    storage.reject_study(pid, reviewed_by="alice")

    # Re-run at the same threshold — same candidate would be kept
    # again, but the rejection must persist.
    with SSOARSource(from_file=FIXTURE) as src:
        run_one(source=src, topic=klima_topic, storage=storage, min_score=0.0001)
    row = storage.get_study(pid)
    assert row["status"] == "rejected"
    assert row["reviewed_by"] == "alice"


def test_promoted_status_is_sticky_across_reruns(
    storage: PostgresStorage, klima_topic
) -> None:
    """A promoted candidate stays kept even if a later run wouldn't keep it."""
    with SSOARSource(from_file=FIXTURE) as src:
        run_one(source=src, topic=klima_topic, storage=storage, min_score=0.4)
    pending = storage.list_studies(topic_id="klima", status="pending", limit=1)
    pid = pending[0]["id"]
    storage.promote_study(pid, reviewed_by="alice")

    # Re-run at the same threshold — that candidate would be pending
    # again, but the promotion must persist.
    with SSOARSource(from_file=FIXTURE) as src:
        run_one(source=src, topic=klima_topic, storage=storage, min_score=0.4)
    row = storage.get_study(pid)
    assert row["status"] == "kept"
    assert row["reviewed_by"] == "alice"


def test_promote_unknown_id_returns_false(storage: PostgresStorage) -> None:
    assert storage.promote_study("0" * 64, reviewed_by="alice") is False


def test_reject_already_rejected_returns_false(
    storage: PostgresStorage, klima_topic
) -> None:
    with SSOARSource(from_file=FIXTURE) as src:
        run_one(source=src, topic=klima_topic, storage=storage)
    kept = storage.list_studies(topic_id="klima", status="kept", limit=1)
    pid = kept[0]["id"]
    storage.reject_study(pid, reviewed_by="alice")
    # Second reject is a no-op
    assert storage.reject_study(pid, reviewed_by="bob") is False


# --------------------------------------------------------------------------
# Status report surfaces pending / rejected counts
# --------------------------------------------------------------------------


def test_status_report_includes_pending_and_rejected(
    storage: PostgresStorage, klima_topic
) -> None:
    from study_scraper.status import build_status, format_text

    with SSOARSource(from_file=FIXTURE) as src:
        run_one(source=src, topic=klima_topic, storage=storage, min_score=0.4)
    # Promote one, reject another so all 3 buckets are non-empty.
    pending = storage.list_studies(topic_id="klima", status="pending", limit=2)
    storage.promote_study(pending[0]["id"], reviewed_by="alice")
    storage.reject_study(pending[1]["id"], reviewed_by="alice")

    report = build_status(storage)
    assert report.pending_count >= 1
    assert report.rejected_count == 1
    assert report.kept_count >= 1
    text = format_text(report)
    assert "kept/pending/rejected" in text


def test_status_per_topic_counts_kept_only(
    storage: PostgresStorage, klima_topic
) -> None:
    """Per-topic counts must reflect only `kept`, not `pending`."""
    from study_scraper.status import build_status

    with SSOARSource(from_file=FIXTURE) as src:
        run_one(source=src, topic=klima_topic, storage=storage, min_score=0.4)
    report = build_status(storage)
    kept_n = len(storage.list_studies(topic_id="klima", status="kept", limit=100))
    assert report.studies_per_topic.get("klima") == kept_n


# --------------------------------------------------------------------------
# CLI commands
# --------------------------------------------------------------------------


@pytest.fixture()
def runner(monkeypatch) -> CliRunner:
    # study_scraper.config caches Settings() process-wide; reset it
    # so monkeypatch.setenv("POSTGRES_URL", ...) actually takes effect.
    import study_scraper.config as _cfg
    monkeypatch.setattr(_cfg, "_settings", None)
    return CliRunner()


def test_cli_review_pending(
    storage: PostgresStorage, klima_topic, runner: CliRunner, monkeypatch
) -> None:
    monkeypatch.setenv("POSTGRES_URL", TEST_DSN)
    with SSOARSource(from_file=FIXTURE) as src:
        run_one(source=src, topic=klima_topic, storage=storage, min_score=0.4)
    result = runner.invoke(app, ["review", "pending", "--topic", "klima"])
    assert result.exit_code == 0, result.output
    # Expect at least one pending Energiewende row
    assert "Energiewende" in result.output


def test_cli_review_promote_and_reject(
    storage: PostgresStorage, klima_topic, runner: CliRunner, monkeypatch
) -> None:
    monkeypatch.setenv("POSTGRES_URL", TEST_DSN)
    with SSOARSource(from_file=FIXTURE) as src:
        run_one(source=src, topic=klima_topic, storage=storage, min_score=0.4)
    pending = storage.list_studies(topic_id="klima", status="pending", limit=2)
    p_promote, p_reject = pending[0]["id"], pending[1]["id"]

    out = runner.invoke(
        app, ["review", "promote", p_promote, "--by", "alice"]
    )
    assert out.exit_code == 0 and "promoted" in out.output

    out = runner.invoke(
        app, ["review", "reject", p_reject, "--by", "alice", "--reason", "test"]
    )
    assert out.exit_code == 0 and "rejected" in out.output

    assert storage.get_study(p_promote)["status"] == "kept"
    assert storage.get_study(p_reject)["status"] == "rejected"


def test_cli_list_status_flag(
    storage: PostgresStorage, klima_topic, runner: CliRunner, monkeypatch
) -> None:
    monkeypatch.setenv("POSTGRES_URL", TEST_DSN)
    with SSOARSource(from_file=FIXTURE) as src:
        run_one(source=src, topic=klima_topic, storage=storage, min_score=0.4)
    out = runner.invoke(app, ["list", "--topic", "klima", "--status", "pending"])
    assert out.exit_code == 0
    assert "Energiewende" in out.output
