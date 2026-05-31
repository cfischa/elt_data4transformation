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

            cur.execute(
                f"""
                SELECT
                    COUNT(*)                                     AS total,
                    COUNT(*) FILTER (WHERE errors = 0)           AS successful,
                    COUNT(*) FILTER (WHERE errors > 0)           AS failed,
                    COALESCE(SUM(candidates_seen), 0)            AS seen,
                    COALESCE(SUM(candidates_kept), 0)            AS kept
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

            cur.execute(
                f"""
                SELECT id, source_id, topic_id, started_at, finished_at,
                       candidates_seen, candidates_kept, errors, parameters
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

    return StatusReport(
        generated_at=datetime.now(timezone.utc),
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
            err_flag = "ERR" if (r.get("errors") or 0) > 0 else "ok "
            lines.append(
                f"    {err_flag}  {r['source_id']:<10} {r['topic_id']:<22}  "
                f"seen={r['candidates_seen']:>4}  kept={r['candidates_kept']:>4}  "
                f"errors={r['errors']:>2}   "
                f"{r['started_at'].isoformat(timespec='seconds')}"
            )
    else:
        lines.append("    (none)")
    return "\n".join(lines)
