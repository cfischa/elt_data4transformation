"""Lake browser — inspect rows in `source_records` (A14).

Per A14, structured-data sources write payloads as-is into the lake.
This page is a read-only browser for that lake: filter by source /
topic / status, expand a row to inspect its raw payload jsonb, and
follow the canonical_url back to the publisher.

Out of scope (deliberately, until the operator has a use case):
  - editing payloads
  - bulk promote/reject (the per-row Review page handles individual
    cases; lake records inherit Q12 status semantics)
  - SQL view editor (views are migrations, not dock features)
"""

from __future__ import annotations

import json
from typing import Optional

import streamlit as st

from study_scraper.console._shared import storage_or_error


st.set_page_config(page_title="Study scraper — lake", layout="wide")
st.title("Lake — source_records browser")
st.caption(
    "Raw structured-data records ingested via `study_scraper ingest "
    "--source <id>`. Payloads are preserved as-is per DECISIONS.md A14; "
    "per-source typed projections live as SQL views and are queryable "
    "via the `view` CLI command."
)

storage = storage_or_error()
if storage is None:
    st.stop()

# Surface available sources + formats from the lake itself, so filter
# options match what's actually ingested.
with storage.connection() as conn:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT source_id "
            "FROM   study_scraper.source_records "
            "ORDER  BY source_id"
        )
        sources = [r["source_id"] for r in cur.fetchall()]
        cur.execute(
            "SELECT DISTINCT format "
            "FROM   study_scraper.source_records "
            "ORDER  BY format"
        )
        formats = [r["format"] for r in cur.fetchall()]
        cur.execute(
            "SELECT DISTINCT unnest(topic_ids) AS t "
            "FROM   study_scraper.source_records "
            "WHERE  array_length(topic_ids, 1) > 0 "
            "ORDER  BY t"
        )
        topics = [r["t"] for r in cur.fetchall()]

col_a, col_b, col_c, col_d = st.columns(4)
with col_a:
    source_filter: Optional[str] = st.selectbox(
        "source", ["(all)"] + sources, index=0
    )
with col_b:
    format_filter: Optional[str] = st.selectbox(
        "format", ["(all)"] + formats, index=0
    )
with col_c:
    topic_filter: Optional[str] = st.selectbox(
        "topic", ["(all)"] + topics, index=0
    )
with col_d:
    status_filter: str = st.selectbox(
        "status", ["kept", "pending", "rejected", "all"], index=0
    )

limit = st.slider("limit", min_value=10, max_value=200, value=50, step=10)

source_arg = None if source_filter == "(all)" else source_filter
format_arg = None if format_filter == "(all)" else format_filter
topic_arg = None if topic_filter == "(all)" else topic_filter
status_arg = None if status_filter == "all" else status_filter

rows = storage.list_source_records(
    source_id=source_arg,
    topic_id=topic_arg,
    format=format_arg,
    status=status_arg,
    limit=limit,
)

st.write(f"showing {len(rows)} record(s)")

if not rows:
    st.info(
        "No records match. Ingest something first, e.g. "
        "`python -m study_scraper ingest --source dawum --from-file ...`."
    )
    st.stop()

# Top-line stats for the current filter.
with storage.connection() as conn:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT format, COUNT(*) AS c "
            "FROM   study_scraper.source_records "
            "WHERE  status = COALESCE(%s, status) "
            "  AND  (%s IS NULL OR source_id = %s) "
            "GROUP  BY format ORDER BY c DESC",
            (status_arg, source_arg, source_arg),
        )
        format_counts = {r["format"]: int(r["c"]) for r in cur.fetchall()}

if format_counts:
    st.markdown("**totals (current source/status filter):**")
    cols = st.columns(min(4, len(format_counts)))
    for col, (fmt_name, n) in zip(cols, format_counts.items()):
        col.metric(fmt_name, n)

st.divider()

# Per-record expander.
for row in rows:
    rid = row["id"]
    short = rid[:12]
    src = row.get("source_id")
    fmt = row.get("format")
    label = row.get("source_record_id") or rid[:16]
    title = f"`{short}`  [{src}/{fmt}]  {label}"
    with st.expander(title, expanded=False):
        st.markdown(f"**canonical_url:** {row.get('canonical_url')}")
        if row.get("license"):
            st.markdown(f"**license:** {row['license']}")
        if row.get("topic_ids"):
            st.markdown(
                f"**topic_ids:** {', '.join(row['topic_ids'])}"
            )
        st.markdown(f"**fetched_at:** {row.get('fetched_at')}")
        st.markdown(f"**status:** `{row.get('status')}`")
        # The full payload isn't returned by list_source_records (it
        # would bloat the index). Fetch on demand when expanded.
        if st.checkbox(
            "show full payload (JSON)",
            key=f"show_{rid}",
            value=False,
        ):
            with storage.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT payload, payload_uri "
                        "FROM   study_scraper.source_records "
                        "WHERE  id = %s",
                        (rid,),
                    )
                    full = cur.fetchone()
            if full and full.get("payload") is not None:
                st.code(
                    json.dumps(full["payload"], indent=2, ensure_ascii=False),
                    language="json",
                )
            elif full and full.get("payload_uri"):
                st.markdown(
                    f"**binary payload at:** `{full['payload_uri']}`"
                )
            else:
                st.write("(no payload)")

st.caption(
    "Tip: per-source typed projections live as SQL views. From the CLI: "
    "`python -m study_scraper view dawum_polls` / `dawum_poll_results`. "
    "Add new views in `study_scraper/migrations/`."
)
