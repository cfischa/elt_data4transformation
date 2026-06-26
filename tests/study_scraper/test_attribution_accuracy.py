"""Accuracy fixes F1 (source-span grounding), F3 (German stance mapping),
F5 (distribution sanity). Pure-function tests over parse_response — no SDK,
no network, no DB."""

from __future__ import annotations

import json

from study_scraper.extractors.llm_v1 import parse_response


SID = "s" * 64


def _resp(items):
    return json.dumps({"attributions": items})


# --- F1: source-span grounding -------------------------------------------


def test_grounded_span_kept_full_confidence() -> None:
    src = "TITLE: Klima\nABSTRACT:\n62% befürworten ein Klimaschutzgesetz."
    attrs = parse_response(_resp([
        {"question": "Stricter climate law", "position": "support",
         "percentage": 62, "population": None, "confidence": 0.9,
         "source_span": "62% befürworten ein Klimaschutzgesetz"},
    ]), study_id=SID, source_text=src)
    assert len(attrs) == 1
    assert attrs[0].grounded is True
    assert attrs[0].confidence == 0.9  # untouched
    assert attrs[0].raw["grounded"] is True


def test_ungrounded_span_flagged_and_confidence_capped() -> None:
    src = "TITLE: Klima\nABSTRACT:\n62% befürworten ein Klimaschutzgesetz."
    attrs = parse_response(_resp([
        {"question": "Nuclear phase-out", "position": "oppose",
         "percentage": 88, "population": None, "confidence": 0.95,
         "source_span": "88% lehnen den Atomausstieg ab"},  # NOT in source
    ]), study_id=SID, source_text=src)
    assert len(attrs) == 1
    assert attrs[0].grounded is False          # hallucination caught
    assert attrs[0].confidence == 0.3          # capped
    assert attrs[0].raw["grounded"] is False


def test_grounding_is_whitespace_and_case_insensitive() -> None:
    src = "ABSTRACT:\n62 %   BEFÜRWORTEN   das   Gesetz."
    attrs = parse_response(_resp([
        {"question": "Q", "position": "support", "percentage": 62,
         "population": None, "confidence": 0.8,
         "source_span": "62 % befürworten das Gesetz"},
    ]), study_id=SID, source_text=src)
    assert attrs[0].grounded is True


def test_no_source_text_means_unchecked() -> None:
    attrs = parse_response(_resp([
        {"question": "Q", "position": "support", "percentage": 62,
         "population": None, "confidence": 0.8, "source_span": "anything"},
    ]), study_id=SID)  # no source_text
    assert attrs[0].grounded is None
    assert attrs[0].confidence == 0.8  # not capped when unchecked


# --- F3: German stance mapping (safety net beyond the enum) ---------------


def test_german_verb_position_mapped() -> None:
    attrs = parse_response(_resp([
        {"question": "Q", "position": "befürworten", "percentage": 55,
         "population": None, "confidence": 0.7, "source_span": ""},
    ]), study_id=SID)
    assert attrs[0].position == "support"


def test_german_ablehnen_maps_oppose() -> None:
    attrs = parse_response(_resp([
        {"question": "Q", "position": "ablehnen", "percentage": 36,
         "population": None, "confidence": 0.7, "source_span": ""},
    ]), study_id=SID)
    assert attrs[0].position == "oppose"


def test_unknown_position_still_unspecified() -> None:
    attrs = parse_response(_resp([
        {"question": "Q", "position": "banana", "percentage": 1,
         "population": None, "confidence": 0.5, "source_span": ""},
    ]), study_id=SID)
    assert attrs[0].position == "unspecified"


# --- F5: distribution sanity ---------------------------------------------


def test_distribution_ok_when_sums_under_100() -> None:
    attrs = parse_response(_resp([
        {"question": "Tempolimit", "position": "support", "percentage": 60,
         "population": None, "confidence": 0.9, "source_span": ""},
        {"question": "Tempolimit", "position": "oppose", "percentage": 30,
         "population": None, "confidence": 0.9, "source_span": ""},
    ]), study_id=SID)
    assert all(a.raw["distribution_check"] is True for a in attrs)


def test_distribution_flagged_when_over_120() -> None:
    attrs = parse_response(_resp([
        {"question": "Tempolimit", "position": "support", "percentage": 80,
         "population": None, "confidence": 0.9, "source_span": ""},
        {"question": "Tempolimit", "position": "oppose", "percentage": 70,
         "population": None, "confidence": 0.9, "source_span": ""},
    ]), study_id=SID)
    assert all(a.raw["distribution_check"] is False for a in attrs)


def test_distribution_is_per_question() -> None:
    attrs = parse_response(_resp([
        {"question": "A", "position": "support", "percentage": 90,
         "population": None, "confidence": 0.9, "source_span": ""},
        {"question": "A", "position": "oppose", "percentage": 90,
         "population": None, "confidence": 0.9, "source_span": ""},
        {"question": "B", "position": "support", "percentage": 50,
         "population": None, "confidence": 0.9, "source_span": ""},
    ]), study_id=SID)
    by_q = {a.question: a.raw["distribution_check"] for a in attrs}
    assert by_q["A"] is False
    assert by_q["B"] is True


def test_source_record_id_target() -> None:
    attrs = parse_response(_resp([
        {"question": "Q", "position": "support", "percentage": 50,
         "population": None, "confidence": 0.8, "source_span": ""},
    ]), source_record_id="r" * 64)
    assert attrs[0].source_record_id == "r" * 64
    assert attrs[0].study_id is None
