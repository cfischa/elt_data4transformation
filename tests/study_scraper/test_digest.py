"""Tests for monitoring v1: watches storage + the digest engine.

Digest-engine tests run against a fake in-memory storage (pure);
watch-table round-trip tests gate on STUDY_SCRAPER_TEST_DSN.
"""

from __future__ import annotations

import datetime
import os
from typing import Any, Dict, List, Optional

import pytest

from study_scraper.digest import (
    SHIFT_POINTS,
    digest_watch,
    format_digest_markdown,
    run_digest,
)

TODAY = datetime.date(2026, 7, 1)


def _finding(q, pos, pct, *, date=None, n=None):
    return {
        "question": q, "position": pos, "percentage": pct,
        "publication_date": date or datetime.date(2026, 1, 1),
        "sample_size": n, "confidence": 0.9,
    }


class FakeStorage:
    """In-memory stand-in exposing the digest engine's storage surface."""

    def __init__(self, findings: List[Dict[str, Any]]) -> None:
        self.findings = findings
        self.snapshots: Dict[int, List[Dict[str, Any]]] = {}
        self.watches: List[Dict[str, Any]] = [
            {"id": 1, "query": "", "label": "Everything", "since_year": None,
             "active": True}
        ]

    def list_watches(self, *, active_only: bool = True):
        return [w for w in self.watches if w["active"] or not active_only]

    def search_attributions_semantic(self, *, query: str, limit: int = 500,
                                    since: Optional[int] = None):
        rows = [f for f in self.findings if query.lower() in f["question"].lower()]
        if since is not None:
            rows = [r for r in rows
                    if r.get("publication_date")
                    and r["publication_date"].year >= since]
        return rows[:limit]

    def latest_watch_snapshot(self, watch_id: int):
        snaps = self.snapshots.get(watch_id) or []
        return snaps[-1] if snaps else None

    def save_watch_snapshot(self, watch_id: int, *, findings_count: int,
                            payload: List[Dict[str, Any]]):
        self.snapshots.setdefault(watch_id, []).append(
            {"watch_id": watch_id, "findings_count": findings_count,
             "payload": payload}
        )


def test_first_run_stores_baseline_and_reports_no_shift() -> None:
    store = FakeStorage([_finding("Stricter climate laws", "support", 62)])
    [d] = run_digest(store, today=TODAY)
    assert d.first_run and not d.shifts and not d.new_questions
    assert store.snapshots[1][0]["findings_count"] == 1


def test_shift_detected_above_threshold(_pct_from=55.0, _pct_to=61.0) -> None:
    store = FakeStorage([_finding("Return to nuclear power", "support", _pct_from)])
    run_digest(store, today=TODAY)
    # New poll replaces the old one → weighted pct moves.
    store.findings = [_finding("Return to nuclear power", "support", _pct_to)]
    [d] = run_digest(store, today=TODAY)
    assert len(d.shifts) == 1
    s = d.shifts[0]
    assert s["from_pct"] == _pct_from and s["to_pct"] == _pct_to
    assert abs(s["delta"]) >= SHIFT_POINTS


def test_small_movement_not_reported() -> None:
    store = FakeStorage([_finding("Q about pension", "support", 70)])
    run_digest(store, today=TODAY)
    store.findings = [_finding("Q about pension", "support", 72)]
    [d] = run_digest(store, today=TODAY)
    assert d.shifts == []


def test_shift_matches_cluster_across_phrasing_drift() -> None:
    # Label drift: the DE phrasing wins the label in run 2, but the
    # cluster must still match run 1's snapshot fuzzily.
    store = FakeStorage([_finding("Return to nuclear power", "support", 55)])
    run_digest(store, today=TODAY)
    store.findings = [
        _finding("Atomausstieg rückgängig machen", "support", 65),
        _finding("Atomausstieg rückgängig machen", "support", 65),
    ]
    [d] = run_digest(store, today=TODAY)
    assert len(d.shifts) == 1
    assert d.shifts[0]["delta"] == 10.0


def test_new_question_reported_after_baseline() -> None:
    store = FakeStorage([_finding("Stricter climate laws", "support", 62)])
    run_digest(store, today=TODAY)
    store.findings.append(_finding("General speed limit", "support", 57))
    [d] = run_digest(store, today=TODAY)
    assert d.new_questions == ["General speed limit"]


def test_watch_since_year_filters_findings() -> None:
    store = FakeStorage([
        _finding("Old poll question", "support", 40,
                 date=datetime.date(2019, 1, 1)),
        _finding("Old poll question", "support", 60,
                 date=datetime.date(2026, 1, 1)),
    ])
    store.watches[0]["since_year"] = 2024
    [d] = run_digest(store, today=TODAY)
    assert d.findings_count == 1


def test_dry_run_saves_nothing() -> None:
    store = FakeStorage([_finding("Q", "support", 50)])
    run_digest(store, today=TODAY, save=False)
    assert store.snapshots == {}


def test_markdown_contains_shift_and_method() -> None:
    store = FakeStorage([_finding("Return to nuclear power", "support", 55)])
    run_digest(store, today=TODAY)
    store.findings = [_finding("Return to nuclear power", "support", 65)]
    digests = run_digest(store, today=TODAY)
    md = format_digest_markdown(
        digests,
        generated_at=datetime.datetime(2026, 7, 1, tzinfo=datetime.timezone.utc),
    )
    assert "# Opinion digest — 2026-07-01" in md
    assert "▲ shift" in md and "55.0% → 65.0%" in md
    assert "Method: weighted mean" in md


def test_markdown_no_watches() -> None:
    md = format_digest_markdown([])
    assert "No active watches" in md


# ---------------------------------------------------------------------------
# Integration: watches table round-trip
# ---------------------------------------------------------------------------

TEST_DSN = os.environ.get("STUDY_SCRAPER_TEST_DSN")


@pytest.mark.skipif(not TEST_DSN, reason="STUDY_SCRAPER_TEST_DSN not set")
def test_watch_roundtrip_and_snapshot() -> None:
    from study_scraper.storage import PostgresStorage

    store = PostgresStorage(TEST_DSN)
    store.migrate()
    with store.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE study_scraper.watch_snapshots, study_scraper.watches")
        conn.commit()

    wid = store.add_watch(query="klima", label="Climate", since_year=2024)
    assert store.list_watches()[0]["query"] == "klima"

    # Re-add updates in place (same id), no duplicate.
    wid2 = store.add_watch(query="klima", label="Klima", since_year=None)
    assert wid2 == wid
    assert len(store.list_watches()) == 1
    assert store.list_watches()[0]["label"] == "Klima"

    assert store.latest_watch_snapshot(wid) is None
    store.save_watch_snapshot(wid, findings_count=3, payload=[
        {"cluster_label": "Q", "position": "support",
         "weighted_pct": 60.0, "n_findings": 3},
    ])
    snap = store.latest_watch_snapshot(wid)
    assert snap["findings_count"] == 3
    assert snap["payload"][0]["weighted_pct"] == 60.0

    assert store.remove_watch(wid) is True
    assert store.list_watches() == []
    assert len(store.list_watches(active_only=False)) == 1
