"""Sources — per-source coverage & health (Phase 5d dock surface).

One row per source (catalog: ssoar/openalex → `studies`; lake:
dawum/gesis/eurostat → `source_records`) with record count, run count,
errors, and last successful run. Plus the recent-run table. Reuses
`build_status()`; one small inline query for per-source last-success.

Read-only.
"""

from __future__ import annotations

import streamlit as st

from study_scraper.console._shared import storage_or_error
from study_scraper.status import build_status


st.set_page_config(page_title="Study scraper — sources", layout="wide")
st.title("Sources — coverage & health")
st.caption(
    "Per-source record counts, run history, and errors. Catalog sources "
    "write to `studies`; lake sources write to `source_records`."
)

storage = storage_or_error()
if storage is None:
    st.stop()

report = build_status(storage, recent_n=50)

c1, c2, c3, c4 = st.columns(4)
c1.metric("kept studies", report.kept_count)
c2.metric("lake records", report.total_source_records)
c3.metric("clean runs", report.successful_runs)
c4.metric("runs with errors", report.failed_runs)

st.divider()

# Per-source last-successful run + cumulative errors (inline, like 3_Lake.py).
with storage.connection() as conn:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT source_id, "
            "       MAX(started_at) FILTER (WHERE errors = 0) AS last_ok, "
            "       MAX(started_at)                           AS last_run, "
            "       COALESCE(SUM(errors), 0)                  AS total_errors "
            "FROM   study_scraper.crawl_runs "
            "GROUP  BY source_id"
        )
        run_stats = {r["source_id"]: r for r in cur.fetchall()}

# Known source kinds (catalog vs lake). Unknown sources still show, kind '?'.
KIND = {
    "ssoar": "catalog", "openalex": "catalog",
    "dawum": "lake", "gesis": "lake", "eurostat": "lake",
}

all_sources = sorted(
    set(report.studies_per_source)
    | set(report.source_records_per_source)
    | set(report.runs_per_source)
    | set(run_stats)
)

rows = []
for sid in all_sources:
    stats = run_stats.get(sid, {})
    last_ok = stats.get("last_ok")
    records = (
        report.studies_per_source.get(sid, 0)
        + report.source_records_per_source.get(sid, 0)
    )
    rows.append({
        "source": sid,
        "kind": KIND.get(sid, "?"),
        "records": records,
        "runs": report.runs_per_source.get(sid, 0),
        "errors": int(stats.get("total_errors") or 0),
        "last successful run": (
            last_ok.isoformat(timespec="seconds") if last_ok else "— never"
        ),
    })

st.subheader("per source")
st.dataframe(rows, use_container_width=True, hide_index=True)

st.subheader("recent runs")
recent = [
    {
        "ok": "ERR" if (r.get("errors") or 0) > 0 else "ok",
        "source": r.get("source_id"),
        "topic": r.get("topic_id"),
        "seen": r.get("candidates_seen"),
        "kept": r.get("candidates_kept"),
        "errors": r.get("errors"),
        "started": (
            r["started_at"].isoformat(timespec="seconds")
            if r.get("started_at") else ""
        ),
    }
    for r in report.recent_runs
]
if recent:
    st.dataframe(recent, use_container_width=True, hide_index=True)
else:
    st.info("No crawl runs yet.")
