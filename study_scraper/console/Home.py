"""Status / coverage dashboard — landing page of the control dock.

Two features only, per DECISIONS.md A11:
  * this page  -> coverage / health overview
  * Topics page -> human edits topic definitions with live preview

Anything bigger needs a new decision.
"""

from __future__ import annotations

import streamlit as st
import pandas as pd

from study_scraper.console._shared import storage_or_error
from study_scraper.status import build_status


st.set_page_config(page_title="Study scraper — status", layout="wide")
st.title("Study scraper — status")

storage = storage_or_error()
if storage is None:
    st.stop()

report = build_status(storage, recent_n=20)

# ---- top-line counters ----------------------------------------------------

col1, col2, col3, col4 = st.columns(4)
col1.metric("Studies", f"{report.total_studies}")
col2.metric("With quantitative data", f"{report.studies_with_quant}")
col3.metric(
    "Crawl runs",
    f"{report.total_runs}",
    delta=(
        None
        if report.failed_runs == 0
        else f"{report.failed_runs} with errors"
    ),
    delta_color=("off" if report.failed_runs == 0 else "inverse"),
)
keep = "—" if report.keep_rate is None else f"{report.keep_rate:.0%}"
col4.metric(
    "Keep rate",
    keep,
    help=(
        f"{report.candidates_kept_total} kept of "
        f"{report.candidates_seen_total} candidates seen"
    ),
)

# ---- per-topic / per-source breakdown -------------------------------------

st.subheader("Coverage")
left, right = st.columns(2)

with left:
    st.markdown("**studies per topic**")
    if report.studies_per_topic:
        df = pd.DataFrame(
            {"topic": list(report.studies_per_topic.keys()),
             "studies": list(report.studies_per_topic.values())}
        )
        st.dataframe(df, hide_index=True, use_container_width=True)
    else:
        st.info("No studies yet. Run an ingest:\n\n"
                "```bash\n"
                "python -m study_scraper run --source ssoar --topic klima\n"
                "```")

with right:
    st.markdown("**studies per source**")
    if report.studies_per_source:
        df = pd.DataFrame(
            {"source": list(report.studies_per_source.keys()),
             "studies": list(report.studies_per_source.values())}
        )
        st.dataframe(df, hide_index=True, use_container_width=True)
    else:
        st.info("No studies yet.")

st.markdown("**studies per (topic × source)**")
if report.studies_per_topic_source:
    df = pd.DataFrame(report.studies_per_topic_source)
    st.dataframe(df, hide_index=True, use_container_width=True)
else:
    st.info("No studies yet.")

# ---- recent runs ----------------------------------------------------------

st.subheader("Recent crawl runs")
if not report.recent_runs:
    st.info("No runs recorded yet.")
else:
    rows = []
    for r in report.recent_runs:
        rows.append(
            {
                "started":   r["started_at"],
                "source":    r["source_id"],
                "topic":     r["topic_id"],
                "seen":      r["candidates_seen"],
                "kept":      r["candidates_kept"],
                "errors":    r["errors"],
                "duration_s": (
                    None
                    if r.get("finished_at") is None
                    else (r["finished_at"] - r["started_at"]).total_seconds()
                ),
            }
        )
    st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)

# ---- failures call-out ----------------------------------------------------

if report.failed_runs > 0:
    st.warning(
        f"{report.failed_runs} run(s) recorded errors. See the `errors` "
        f"column above and check the operator's logs."
    )

st.caption(
    f"Report generated at {report.generated_at.isoformat(timespec='seconds')}. "
    f"This page is read-only; topics are edited on the Topics page."
)
