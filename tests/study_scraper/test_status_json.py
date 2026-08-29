"""`report_to_dict` tests -- no DB needed, `StatusReport` built directly.

`status --json` used `dataclasses.asdict(report)`, which only serializes
plain dataclass fields and silently drops the derived `@property` rates
(`keep_rate`, `attribution_coverage_rate`, ...) that the text/console
output already surfaces. `report_to_dict` is the fix; these tests pin
its output so the two representations can't drift apart again.
"""

from __future__ import annotations

from datetime import datetime, timezone

from study_scraper.status import StatusReport, report_to_dict


def _report(**overrides) -> StatusReport:
    base = dict(
        generated_at=datetime(2026, 8, 28, tzinfo=timezone.utc),
        total_studies=10,
        studies_with_quant=4,
        studies_per_status={"kept": 8, "pending": 2},
        studies_per_topic={"klima": 8},
        studies_per_source={"ssoar": 8},
        studies_per_topic_source=[],
        total_source_records=0,
        source_records_per_source={},
        source_records_per_format={},
        total_runs=5,
        successful_runs=4,
        failed_runs=1,
        runs_per_source={"ssoar": 5},
        recent_runs=[],
        candidates_seen_total=20,
        candidates_kept_total=8,
        duplicates_total=2,
        attribution_last_run_attempts=40,
        attribution_last_run_found=4,
        attribution_queue_size=12,
        total_claims=100,
        total_attributions=10,
    )
    base.update(overrides)
    return StatusReport(**base)


def test_report_to_dict_includes_computed_rates() -> None:
    report = _report()
    payload = report_to_dict(report)

    assert payload["keep_rate"] == report.keep_rate == 0.4
    assert payload["duplicate_rate"] == report.duplicate_rate == 0.1
    assert payload["kept_count"] == report.kept_count == 8
    assert payload["pending_count"] == report.pending_count == 2
    assert payload["rejected_count"] == report.rejected_count == 0
    assert payload["attribution_coverage_rate"] == report.attribution_coverage_rate == 0.1
    assert (
        payload["attribution_last_run_yield_rate"]
        == report.attribution_last_run_yield_rate
        == 0.1
    )
    # Plain dataclass fields still come through as before.
    assert payload["total_studies"] == 10
    assert payload["attribution_queue_size"] == 12


def test_report_to_dict_handles_none_rates() -> None:
    report = _report(
        candidates_seen_total=0,
        candidates_kept_total=0,
        duplicates_total=0,
        total_claims=0,
        total_attributions=0,
        attribution_last_run_attempts=0,
        attribution_last_run_found=0,
    )
    payload = report_to_dict(report)

    assert payload["keep_rate"] is None
    assert payload["duplicate_rate"] is None
    assert payload["attribution_coverage_rate"] is None
    assert payload["attribution_last_run_yield_rate"] is None
