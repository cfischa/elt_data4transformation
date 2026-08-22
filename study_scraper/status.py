"""Aggregated counters across the studies / crawl_runs tables.

Used by:
- `python -m study_scraper status` (CLI, cron-friendly)
- `study_scraper/console` (Streamlit control UI)

Read-only. Returns plain dicts so the same data can be rendered as text,
JSON, markdown, or a dataframe without leaking psycopg types upward.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from study_scraper.storage import PostgresStorage


SCHEMA = "study_scraper"


@dataclass
class StatusReport:
    generated_at: datetime
    total_studies: int
    studies_with_quant: int
    studies_per_status: Dict[str, int]
    studies_per_topic: Dict[str, int]
    studies_per_source: Dict[str, int]
    studies_per_topic_source: List[Dict[str, Any]]
    # Lake-side (source_records) counters added per A14 + cleanup
    # 2026-05-31 -- so `status` reports both catalog and lake coverage.
    total_source_records: int
    source_records_per_source: Dict[str, int]
    source_records_per_format: Dict[str, int]
    total_runs: int
    successful_runs: int
    failed_runs: int
    runs_per_source: Dict[str, int]
    recent_runs: List[Dict[str, Any]]
    candidates_seen_total: int
    candidates_kept_total: int
    # Candidates seen this run that deduped onto an already-stored study
    # (DOI or title-similarity match, see storage.upsert_study) rather
    # than landing as a new row -- the "waste" of a re-fetch (issue #82).
    duplicates_total: int = 0
    # How long since the attribution pass last actually wrote a row --
    # None if attribution_attempts is empty (never run). A run that
    # completes "successfully" in CI but writes zero attempts (#110)
    # otherwise looks identical to a healthy pipeline in status output.
    attribution_days_since_last_attempt: Optional[float] = None
    # How long since each source's last *clean* crawl run (errors = 0,
    # not aborted) -- None if the source has never had one. Generalizes
    # #110's attribution signal to crawl sources so a silent regression
    # (e.g. bundestag_dip's recurring 401s, #106) shows up here instead
    # of needing a human to diff `crawl_runs` timestamps by hand.
    source_days_since_last_success: Dict[str, Optional[float]] = field(
        default_factory=dict
    )
    # Yield of the most recent attribution run (the latest calendar day
    # present in attribution_attempts) -- #49's monitor updates repeatedly
    # hand-computed "N attempts, M found" from Postgres to track a
    # declining-yield concern distinct from the staleness signal above
    # (a run can be fresh/on-cadence and still find almost nothing).
    attribution_last_run_attempts: int = 0
    attribution_last_run_found: int = 0

    @property
    def attribution_last_run_yield_rate(self) -> Optional[float]:
        if self.attribution_last_run_attempts == 0:
            return None
        return self.attribution_last_run_found / self.attribution_last_run_attempts

    @property
    def pending_count(self) -> int:
        return self.studies_per_status.get("pending", 0)

    @property
    def rejected_count(self) -> int:
        return self.studies_per_status.get("rejected", 0)

    @property
    def kept_count(self) -> int:
        return self.studies_per_status.get("kept", 0)

    @property
    def keep_rate(self) -> Optional[float]:
        if self.candidates_seen_total == 0:
            return None
        return self.candidates_kept_total / self.candidates_seen_total

    @property
    def duplicate_rate(self) -> Optional[float]:
        """Share of seen candidates that were already-known studies
        (re-fetches) rather than new discoveries -- crawl spend that
        yielded no new coverage."""
        if self.candidates_seen_total == 0:
            return None
        return self.duplicates_total / self.candidates_seen_total


def build_status(storage: PostgresStorage, *, recent_n: int = 10) -> StatusReport:
    with storage.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS c FROM {SCHEMA}.studies")
            total_studies = int(cur.fetchone()["c"])

            cur.execute(
                f"SELECT COUNT(*) AS c FROM {SCHEMA}.studies "
                f"WHERE has_quantitative_data"
            )
            studies_with_quant = int(cur.fetchone()["c"])

            cur.execute(
                f"SELECT status, COUNT(*) AS c "
                f"FROM {SCHEMA}.studies GROUP BY status"
            )
            studies_per_status = {row["status"]: int(row["c"]) for row in cur.fetchall()}

            # Per-topic / per-source breakdowns only count `kept`
            # studies (the user-facing coverage). `pending` and
            # `rejected` are surfaced separately via studies_per_status.
            cur.execute(
                f"""
                SELECT unnest(topic_ids) AS topic_id, COUNT(*) AS c
                FROM   {SCHEMA}.studies
                WHERE  status = 'kept'
                GROUP  BY topic_id
                ORDER  BY c DESC
                """
            )
            studies_per_topic = {row["topic_id"]: int(row["c"]) for row in cur.fetchall()}

            cur.execute(
                f"""
                SELECT source_id, COUNT(*) AS c
                FROM   {SCHEMA}.studies
                WHERE  status = 'kept'
                GROUP  BY source_id
                ORDER  BY c DESC
                """
            )
            studies_per_source = {row["source_id"]: int(row["c"]) for row in cur.fetchall()}

            cur.execute(
                f"""
                SELECT unnest(topic_ids) AS topic_id, source_id, COUNT(*) AS c
                FROM   {SCHEMA}.studies
                WHERE  status = 'kept'
                GROUP  BY topic_id, source_id
                ORDER  BY c DESC
                """
            )
            studies_per_topic_source = [
                {"topic_id": row["topic_id"], "source_id": row["source_id"], "count": int(row["c"])}
                for row in cur.fetchall()
            ]

            # A run that raised out of `iter_candidates` (e.g. an
            # unhandled 401) leaves `finished_at` NULL and `errors` at 0
            # (pipeline.py's per-candidate error counter never ran) and
            # is marked with `notes` starting `aborted:` -- count it as
            # failed too, so a source that dies on every request doesn't
            # read as "successful" here (#48). `notes LIKE 'aborted:%'`
            # (rather than `finished_at IS NULL` alone) is required
            # because `ingest.py::run_lake_ingest` also leaves
            # `finished_at` NULL for the entire duration of a healthy,
            # still-running lake ingest -- that in-progress row must not
            # be flagged as failed.
            cur.execute(
                f"""
                SELECT
                    COUNT(*)                                                AS total,
                    COUNT(*) FILTER (
                        WHERE errors = 0
                          AND NOT (finished_at IS NULL
                                    AND COALESCE(notes, '') LIKE 'aborted:%'))
                                                                             AS successful,
                    COUNT(*) FILTER (
                        WHERE errors > 0
                           OR (finished_at IS NULL
                                AND COALESCE(notes, '') LIKE 'aborted:%'))
                                                                             AS failed,
                    COALESCE(SUM(candidates_seen), 0)                       AS seen,
                    COALESCE(SUM(candidates_kept), 0)                       AS kept,
                    COALESCE(SUM((parameters->>'duplicates')::int), 0)      AS duplicates
                FROM {SCHEMA}.crawl_runs
                """
            )
            run_row = cur.fetchone()

            cur.execute(
                f"""
                SELECT source_id, COUNT(*) AS c
                FROM   {SCHEMA}.crawl_runs
                GROUP  BY source_id
                ORDER  BY c DESC
                """
            )
            runs_per_source = {row["source_id"]: int(row["c"]) for row in cur.fetchall()}

            # Per-source staleness (#115, generalizing #110): the most
            # recent *clean* run per source, same "clean" definition as
            # the successful/failed split above (errors = 0 and not
            # aborted).
            cur.execute(
                f"""
                SELECT source_id, MAX(started_at) AS last_success
                FROM   {SCHEMA}.crawl_runs
                WHERE  errors = 0
                  AND  NOT (finished_at IS NULL
                            AND COALESCE(notes, '') LIKE 'aborted:%')
                GROUP  BY source_id
                """
            )
            last_success_per_source = {
                row["source_id"]: row["last_success"] for row in cur.fetchall()
            }

            cur.execute(
                f"""
                SELECT id, source_id, topic_id, started_at, finished_at,
                       candidates_seen, candidates_kept, errors, parameters,
                       notes
                FROM   {SCHEMA}.crawl_runs
                ORDER  BY started_at DESC
                LIMIT  %s
                """,
                (recent_n,),
            )
            recent_runs = [dict(row) for row in cur.fetchall()]
            for row in recent_runs:
                # Ensure JSON-friendly id and timestamps.
                row["id"] = str(row["id"])

            # Lake (source_records) counts -- A14.
            cur.execute(
                f"SELECT COUNT(*) AS c FROM {SCHEMA}.source_records "
                f"WHERE status = 'kept'"
            )
            total_source_records = int(cur.fetchone()["c"])
            cur.execute(
                f"""
                SELECT source_id, COUNT(*) AS c
                FROM   {SCHEMA}.source_records
                WHERE  status = 'kept'
                GROUP  BY source_id
                ORDER  BY c DESC
                """
            )
            source_records_per_source = {
                row["source_id"]: int(row["c"]) for row in cur.fetchall()
            }
            cur.execute(
                f"""
                SELECT format, COUNT(*) AS c
                FROM   {SCHEMA}.source_records
                WHERE  status = 'kept'
                GROUP  BY format
                ORDER  BY c DESC
                """
            )
            source_records_per_format = {
                row["format"]: int(row["c"]) for row in cur.fetchall()
            }

            # Attribution staleness -- #110.
            cur.execute(
                f"SELECT MAX(attempted_at) AS last_attempt "
                f"FROM {SCHEMA}.attribution_attempts"
            )
            last_attribution_attempt = cur.fetchone()["last_attempt"]

            # Attribution yield of the most recent run -- #119. Attempts
            # are written in one batch per invocation, so the latest
            # calendar day present is "the last run" even though there's
            # no explicit run id column on attribution_attempts.
            cur.execute(
                f"""
                SELECT COUNT(*) AS attempts,
                       COALESCE(SUM(found), 0) AS found
                FROM   {SCHEMA}.attribution_attempts
                WHERE  attempted_at::date = (
                    SELECT MAX(attempted_at::date) FROM {SCHEMA}.attribution_attempts
                )
                """
            )
            yield_row = cur.fetchone()
            attribution_last_run_attempts = int(yield_row["attempts"] or 0)
            attribution_last_run_found = int(yield_row["found"] or 0)

    generated_at = datetime.now(timezone.utc)
    attribution_days_since_last_attempt = (
        (generated_at - last_attribution_attempt).total_seconds() / 86400.0
        if last_attribution_attempt is not None
        else None
    )
    source_days_since_last_success = {
        source_id: (
            (generated_at - last_success_per_source[source_id]).total_seconds()
            / 86400.0
            if last_success_per_source.get(source_id) is not None
            else None
        )
        for source_id in runs_per_source
    }

    return StatusReport(
        generated_at=generated_at,
        total_studies=total_studies,
        studies_with_quant=studies_with_quant,
        studies_per_status=studies_per_status,
        studies_per_topic=studies_per_topic,
        studies_per_source=studies_per_source,
        studies_per_topic_source=studies_per_topic_source,
        total_source_records=total_source_records,
        source_records_per_source=source_records_per_source,
        source_records_per_format=source_records_per_format,
        total_runs=int(run_row["total"]),
        successful_runs=int(run_row["successful"]),
        failed_runs=int(run_row["failed"]),
        runs_per_source=runs_per_source,
        recent_runs=recent_runs,
        candidates_seen_total=int(run_row["seen"]),
        candidates_kept_total=int(run_row["kept"]),
        duplicates_total=int(run_row["duplicates"]),
        attribution_days_since_last_attempt=attribution_days_since_last_attempt,
        source_days_since_last_success=source_days_since_last_success,
        attribution_last_run_attempts=attribution_last_run_attempts,
        attribution_last_run_found=attribution_last_run_found,
    )


def format_text(report: StatusReport) -> str:
    """Render a `StatusReport` as a plain-text block for the CLI."""
    lines: List[str] = []
    lines.append(f"study scraper status @ {report.generated_at.isoformat(timespec='seconds')}")
    lines.append("=" * 64)
    lines.append(
        f"  studies (kept/pending/rejected): "
        f"{report.kept_count} / {report.pending_count} / {report.rejected_count}  "
        f"(total {report.total_studies})"
    )
    lines.append(f"  with quantitative data     : {report.studies_with_quant}")
    lines.append(f"  total crawl runs           : {report.total_runs} "
                 f"({report.successful_runs} clean / {report.failed_runs} with errors)")
    lines.append(f"  candidates seen / kept     : "
                 f"{report.candidates_seen_total} / {report.candidates_kept_total}"
                 + (f"  ({report.keep_rate:.1%})" if report.keep_rate is not None else ""))
    lines.append(f"  duplicates (already known) : {report.duplicates_total}"
                 + (f"  ({report.duplicate_rate:.1%} of seen)"
                    if report.duplicate_rate is not None else ""))
    if report.attribution_days_since_last_attempt is None:
        lines.append("  attribution last attempt   : never")
    else:
        lines.append(
            "  attribution last attempt   : "
            f"{report.attribution_days_since_last_attempt:.1f} days ago"
        )
    if report.attribution_last_run_attempts:
        lines.append(
            "  attribution last run yield : "
            f"{report.attribution_last_run_found}/{report.attribution_last_run_attempts}"
            f"  ({report.attribution_last_run_yield_rate:.1%})"
        )
    lines.append("")
    lines.append("  studies per topic:")
    if report.studies_per_topic:
        for topic_id, n in report.studies_per_topic.items():
            lines.append(f"    {topic_id:<28} {n}")
    else:
        lines.append("    (none)")
    lines.append("")
    lines.append("  studies per source (catalog):")
    if report.studies_per_source:
        for source_id, n in report.studies_per_source.items():
            lines.append(f"    {source_id:<28} {n}")
    else:
        lines.append("    (none)")
    lines.append("")
    lines.append("  crawl staleness per source (days since last clean run):")
    if report.source_days_since_last_success:
        for source_id, days in sorted(
            report.source_days_since_last_success.items(),
            key=lambda kv: (kv[1] is None, -(kv[1] or 0)),
        ):
            shown = "never" if days is None else f"{days:.1f}"
            lines.append(f"    {source_id:<28} {shown}")
    else:
        lines.append("    (none)")
    lines.append("")
    lines.append(
        f"  lake (source_records, kept): {report.total_source_records}"
    )
    if report.source_records_per_source:
        for source_id, n in report.source_records_per_source.items():
            fmt = report.source_records_per_format
            lines.append(f"    {source_id:<28} {n}")
        # Per-format breakdown (mostly useful when a single source
        # emits multiple formats; e.g. dawum_survey_json today).
        lines.append("  lake per format:")
        for fmt_name, n in report.source_records_per_format.items():
            lines.append(f"    {fmt_name:<28} {n}")
    else:
        lines.append("    (none)")
    lines.append("")
    lines.append("  recent runs (newest first):")
    if report.recent_runs:
        for r in report.recent_runs:
            aborted = r.get("finished_at") is None and (r.get("notes") or "").startswith("aborted:")
            err_flag = "ERR" if (r.get("errors") or 0) > 0 or aborted else "ok "
            note = f"  ({r['notes']})" if aborted and r.get("notes") else ""
            lines.append(
                f"    {err_flag}  {r['source_id']:<10} {r['topic_id']:<22}  "
                f"seen={r['candidates_seen']:>4}  kept={r['candidates_kept']:>4}  "
                f"errors={r['errors']:>2}   "
                f"{r['started_at'].isoformat(timespec='seconds')}{note}"
            )
    else:
        lines.append("    (none)")
    return "\n".join(lines)
