"""Tests for cross-study finding dedup (pure; no DB)."""

from __future__ import annotations

from study_scraper.findings import dedup_finding_key, dedupe_attributions


def _row(q, pos, pct, conf, grounded=None, title="t"):
    return {
        "question": q, "position": pos, "percentage": pct,
        "confidence": conf, "title": title,
        "raw": {"grounded": grounded} if grounded is not None else {},
    }


def test_key_rounds_percentage_to_whole_point() -> None:
    assert dedup_finding_key("Q", "support", 61.6) == dedup_finding_key("Q", "support", 62.0)
    assert dedup_finding_key("Q", "support", 62) == ("q", "support", 62, "")


def test_key_normalizes_question_whitespace_case() -> None:
    a = dedup_finding_key("  Climate  Law ", "support", 50)
    b = dedup_finding_key("climate law", "support", 50)
    assert a == b


def test_key_none_percentage_distinct_from_value() -> None:
    assert dedup_finding_key("Q", "support", None)[2] is None
    assert dedup_finding_key("Q", "support", None) != dedup_finding_key("Q", "support", 50)


def test_dedupe_keeps_highest_confidence() -> None:
    rows = [
        _row("Climate law", "support", 62, 0.6),
        _row("climate law", "support", 62.0, 0.9),   # dup, higher conf
        _row("Climate law", "support", 61.7, 0.5),   # dup (rounds to 62)
    ]
    out = dedupe_attributions(rows)
    assert len(out) == 1
    assert out[0]["confidence"] == 0.9
    assert out[0]["dup_count"] == 3


def test_dedupe_distinct_positions_kept_separate() -> None:
    rows = [
        _row("Tempolimit", "support", 60, 0.8),
        _row("Tempolimit", "oppose", 30, 0.8),
    ]
    out = dedupe_attributions(rows)
    assert len(out) == 2


def test_dedupe_preserves_first_appearance_order() -> None:
    rows = [
        _row("B question", "support", 50, 0.5),
        _row("A question", "support", 70, 0.5),
        _row("b question", "support", 50.2, 0.9),  # dup of first
    ]
    out = dedupe_attributions(rows)
    assert [r["question"] for r in out] == ["b question", "A question"]
    # first slot kept, but representative is the higher-confidence dup
    assert out[0]["confidence"] == 0.9


def test_dedupe_grounded_breaks_confidence_tie() -> None:
    rows = [
        _row("Q", "support", 50, 0.8, grounded=False),
        _row("Q", "support", 50, 0.8, grounded=True),  # same conf, grounded wins
    ]
    out = dedupe_attributions(rows)
    assert len(out) == 1
    assert out[0]["raw"]["grounded"] is True


def test_dedupe_newer_publication_breaks_confidence_tie() -> None:
    # ROADMAP item B: on equal confidence + groundedness, the newer
    # study's row represents the finding.
    import datetime

    rows = [
        {**_row("Q", "support", 50, 0.8), "title": "old",
         "publication_date": datetime.date(2021, 1, 1)},
        {**_row("Q", "support", 50, 0.8), "title": "new",
         "publication_date": datetime.date(2025, 6, 1)},
    ]
    out = dedupe_attributions(rows)
    assert len(out) == 1
    assert out[0]["title"] == "new"


def test_dedupe_unknown_date_loses_to_dated_row() -> None:
    import datetime

    rows = [
        {**_row("Q", "support", 50, 0.8), "title": "dated",
         "publication_date": datetime.date(2020, 1, 1)},
        {**_row("Q", "support", 50, 0.8), "title": "undated",
         "publication_date": None},
    ]
    out = dedupe_attributions(rows)
    assert out[0]["title"] == "dated"


def test_empty_input() -> None:
    assert dedupe_attributions([]) == []


def test_different_populations_do_not_merge() -> None:
    # Statistical correctness: same question+% among different populations
    # are DIFFERENT findings (e.g. East vs West Germany).
    rows = [
        {**_row("Tempolimit", "support", 60, 0.8), "population": "Ostdeutsche"},
        {**_row("Tempolimit", "support", 60, 0.9), "population": "Westdeutsche"},
    ]
    out = dedupe_attributions(rows)
    assert len(out) == 2


def test_blank_and_none_population_still_merge() -> None:
    rows = [
        {**_row("Q", "support", 50, 0.6), "population": None},
        {**_row("Q", "support", 50, 0.9), "population": "  "},
    ]
    out = dedupe_attributions(rows)
    assert len(out) == 1 and out[0]["confidence"] == 0.9
