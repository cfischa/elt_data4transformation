"""Tests for the auto-reviewer policy (pure; no DB). Coverage-first: the
pending band is always resolved to kept/rejected, biased to keep."""

from __future__ import annotations

from study_scraper.auto_review import decide, _max_score


GOOD_ABSTRACT = "Eine repräsentative Umfrage unter 1000 Befragten zum Klimaschutz."


def test_claims_present_is_kept() -> None:
    decision, why = decide(
        title="Klima", abstract=GOOD_ABSTRACT, claims_count=3, max_score=0.05,
    )
    assert decision == "kept"
    assert "claim" in why


def test_near_threshold_score_is_kept() -> None:
    decision, _ = decide(
        title="Klima", abstract=GOOD_ABSTRACT, claims_count=0, max_score=0.15,
    )
    assert decision == "kept"


def test_borderline_with_text_defaults_to_kept() -> None:
    # coverage-first: real text, low score, no claims -> still keep
    decision, why = decide(
        title="Eine Studie", abstract=GOOD_ABSTRACT, claims_count=0, max_score=0.07,
    )
    assert decision == "kept"
    assert "coverage-first" in why


def test_noise_is_rejected() -> None:
    # almost no text and a near-zero score -> noise
    decision, why = decide(
        title="x", abstract="", claims_count=0, max_score=0.02,
    )
    assert decision == "rejected"
    assert "noise" in why


def test_never_returns_pending() -> None:
    # whatever the inputs, the band is always resolved
    for cc, sc, ab in [(0, 0.0, ""), (0, 0.19, GOOD_ABSTRACT), (5, 0.0, "x")]:
        decision, _ = decide(title="t", abstract=ab, claims_count=cc, max_score=sc)
        assert decision in ("kept", "rejected")


def test_max_score_helper() -> None:
    assert _max_score({"klima": 0.1, "steuern": 0.3}) == 0.3
    assert _max_score({}) is None
    assert _max_score(None) is None
    assert _max_score("nonsense") is None
