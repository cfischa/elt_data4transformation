"""Accuracy fix F4: claims regex must catch German word-form percentages,
not just the '%' sign. Pure-function tests (no DB)."""

from __future__ import annotations

from study_scraper.claims import _normalize_pct_unit, extract_claims_from_text


def _values(text: str, unit: str | None = None):
    claims = extract_claims_from_text(study_id="s", text=text)
    return [c.numeric_value for c in claims if unit is None or c.unit == unit]


def test_prozent_word_is_captured() -> None:
    vals = _values("62 Prozent der Befragten befürworten das Gesetz.", unit="%")
    assert 62.0 in vals


def test_prozent_no_space() -> None:
    vals = _values("Zustimmung lag bei 47Prozent.", unit="%")
    assert 47.0 in vals


def test_v_h_abbreviation() -> None:
    vals = _values("55 v.H. der Wähler lehnen dies ab.", unit="%")
    assert 55.0 in vals


def test_vom_hundert() -> None:
    vals = _values("Rund 30 vom Hundert sind unentschieden.", unit="%")
    assert 30.0 in vals


def test_percent_sign_still_works() -> None:
    vals = _values("62% Zustimmung, 36 % Ablehnung.", unit="%")
    assert 62.0 in vals and 36.0 in vals


def test_prozentpunkte_tagged_pp_not_percent() -> None:
    claims = extract_claims_from_text(
        study_id="s", text="Die Union verliert 3 Prozentpunkte gegenüber Vormonat."
    )
    pp = [c for c in claims if c.unit == "pp"]
    assert any(c.numeric_value == 3.0 for c in pp)
    # ...and it is NOT mislabeled as a level percentage
    assert not any(c.numeric_value == 3.0 and c.unit == "%" for c in claims)


def test_decimal_comma_with_prozent() -> None:
    vals = _values("62,5 Prozent Zustimmung.", unit="%")
    assert 62.5 in vals


def test_normalize_unit_helper() -> None:
    assert _normalize_pct_unit("%") == "%"
    assert _normalize_pct_unit("Prozent") == "%"
    assert _normalize_pct_unit("v.H.") == "%"
    assert _normalize_pct_unit("vom Hundert") == "%"
    assert _normalize_pct_unit("Prozentpunkte") == "pp"
    assert _normalize_pct_unit("Prozentpunkt") == "pp"


def test_over_120_suppressed() -> None:
    # methodology/year-ish big numbers with a unit are still suppressed
    vals = _values("Die Zahl stieg um 250 Prozent.", unit="%")
    assert 250.0 not in vals
