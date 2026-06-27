"""Tests for the accuracy harness (pure; no DB/API). Scores the shipped
sample gold and checks the metrics math + that the accuracy fixes hold."""

from __future__ import annotations

from study_scraper.eval import harness
from study_scraper.eval.harness import (
    eval_attributions,
    eval_claims,
    eval_topics,
    load_jsonl,
)
from study_scraper.topics import load_topics
from study_scraper.config import get_settings


TOPICS = {t.id: t for t in load_topics(get_settings().topics_csv_path)}


def test_topics_sample_perfect_on_shipped_gold() -> None:
    entries = load_jsonl(harness.GOLD_DIR / "topics.jsonl")
    res = eval_topics(entries, TOPICS)
    # F6 morphology means the 4 on-topic inflected titles are caught,
    # and the 2 excluded ones are correctly dropped.
    assert res["recall"] == 1.0
    assert res["precision"] == 1.0
    assert res["fn"] == 0 and res["fp"] == 0


def test_claims_sample_full_capture() -> None:
    entries = load_jsonl(harness.GOLD_DIR / "claims.jsonl")
    res = eval_claims(entries)
    # F4 German percentage words -> all expected figures captured.
    assert res["capture_rate"] == 1.0
    assert res["total"] == 4


def test_attributions_metrics_and_hallucination() -> None:
    entries = load_jsonl(harness.GOLD_DIR / "attributions.jsonl")
    res = eval_attributions(entries)
    # 4 parsed (62 support, 30 oppose, 50 support, 99 oppose);
    # 3 match gold; the 99% oppose span isn't in source -> hallucinated.
    assert res["parsed"] == 4
    assert res["recall"] == 1.0            # all 3 gold found
    assert res["precision"] == 0.75        # 3 of 4 parsed match
    assert res["hallucination_rate"] == 0.25  # F1 caught the fabricated span


def test_run_all_and_report_render() -> None:
    res = harness.run_all(topics_by_id=TOPICS)
    assert set(res) == {"topics", "claims", "attributions"}
    md = harness.format_report(res)
    assert "# Accuracy report" in md
    assert "hallucination rate" in md


def test_eval_topics_empty_is_safe() -> None:
    res = eval_topics([], TOPICS)
    assert res["recall"] is None and res["precision"] is None
