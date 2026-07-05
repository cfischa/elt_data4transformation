"""Tests for poll-of-polls aggregation (ROADMAP D, pure; no DB)."""

from __future__ import annotations

import datetime

from study_scraper.aggregate import (
    aggregate_findings,
    format_answer,
    recency_weight,
    sample_weight,
)

TODAY = datetime.date(2026, 7, 1)


def _row(q, pos, pct, *, date=None, n=None, conf=0.9):
    return {
        "question": q, "position": pos, "percentage": pct,
        "publication_date": date, "sample_size": n, "confidence": conf,
    }


class TestWeights:
    def test_recency_half_life(self) -> None:
        now = recency_weight(datetime.date(2026, 7, 1), TODAY)
        three_years = recency_weight(datetime.date(2023, 7, 2), TODAY)
        assert abs(now - 1.0) < 1e-9
        assert abs(three_years - 0.5) < 0.01

    def test_undated_is_heavily_discounted_but_not_zero(self) -> None:
        w = recency_weight(None, TODAY)
        assert 0.0 < w < 0.25

    def test_sample_weight_reference_and_clamps(self) -> None:
        assert sample_weight(1000) == 1.0
        assert sample_weight(None) == 1.0
        assert sample_weight(10) == 0.3          # clamped floor
        assert sample_weight(10_000_000) == 3.0  # clamped ceiling
        assert 1.9 < sample_weight(4000) < 2.1   # sqrt(4) = 2


class TestAggregate:
    def test_recency_pulls_weighted_mean_toward_newer_poll(self) -> None:
        rows = [
            _row("Stricter climate laws", "support", 44,
                 date=datetime.date(2020, 7, 1)),
            _row("Stricter climate laws", "support", 62,
                 date=datetime.date(2026, 6, 1)),
        ]
        answers = aggregate_findings(rows, today=TODAY)
        assert len(answers) == 1
        agg = answers[0].get("support")
        assert agg is not None
        plain_mean = (44 + 62) / 2
        assert agg.weighted_pct > plain_mean
        assert (agg.min_pct, agg.max_pct) == (44.0, 62.0)
        assert agg.n_findings == 2
        assert (agg.year_min, agg.year_max) == (2020, 2026)

    def test_bigger_sample_pulls_weighted_mean(self) -> None:
        d = datetime.date(2026, 1, 1)
        rows = [
            _row("General speed limit", "support", 50, date=d, n=100),
            _row("General speed limit", "support", 70, date=d, n=10_000),
        ]
        agg = aggregate_findings(rows, today=TODAY)[0].get("support")
        assert agg.weighted_pct > 60
        assert agg.total_sample == 10_100

    def test_positions_kept_separate_and_ordered(self) -> None:
        d = datetime.date(2026, 1, 1)
        rows = [
            _row("Re-enter nuclear energy", "oppose", 36, date=d),
            _row("Re-enter nuclear energy", "support", 55, date=d),
        ]
        answer = aggregate_findings(rows, today=TODAY)[0]
        assert [p.position for p in answer.positions] == ["support", "oppose"]

    def test_de_en_variants_aggregate_into_one_cluster(self) -> None:
        d = datetime.date(2026, 1, 1)
        rows = [
            _row("Return to nuclear power", "support", 55, date=d),
            _row("Atomausstieg rückgängig machen", "support", 65, date=d),
        ]
        answers = aggregate_findings(rows, today=TODAY)
        assert len(answers) == 1
        assert answers[0].get("support").n_findings == 2

    def test_rows_without_percentage_skipped(self) -> None:
        rows = [_row("Q", "support", None)]
        assert aggregate_findings(rows, today=TODAY) == []

    def test_clusters_sorted_by_finding_count(self) -> None:
        d = datetime.date(2026, 1, 1)
        rows = [
            _row("Higher pension level", "support", 70, date=d),
            _row("General speed limit", "support", 55, date=d),
            _row("General speed limit", "oppose", 35, date=d),
        ]
        answers = aggregate_findings(rows, today=TODAY)
        assert answers[0].label == "General speed limit"


class TestFormat:
    def test_format_contains_spread_years_and_sigma_n(self) -> None:
        rows = [
            _row("Stricter climate laws", "support", 44,
                 date=datetime.date(2022, 7, 1), n=6063),
            _row("Stricter climate laws", "support", 62,
                 date=datetime.date(2021, 6, 1), n=1009),
        ]
        text = format_answer(aggregate_findings(rows, today=TODAY)[0])
        assert "Q: Stricter climate laws" in text
        assert "2 polls" in text
        assert "spread 44–62%" in text
        assert "2021–2022" in text
        assert "Σn=7,072" in text

    def test_single_poll_omits_spread(self) -> None:
        rows = [_row("Q", "support", 55, date=datetime.date(2025, 3, 1))]
        text = format_answer(aggregate_findings(rows, today=TODAY)[0])
        assert "spread" not in text
        assert "1 poll" in text
