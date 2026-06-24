"""Review queue — promote or reject pending candidates (Q12).

Pending candidates are studies the topic filter saw, that survived the
exclude short-circuit, but scored below the threshold. They live in the
DB so the operator can decide case-by-case in the dock instead of
having the threshold silently drop them.

Decisions stick across re-runs (see storage.upsert_study sticky rules).
"""

from __future__ import annotations

import os
from typing import Optional

import streamlit as st

from study_scraper.console._shared import storage_or_error


st.set_page_config(page_title="Study scraper — review", layout="wide")
st.title("Review queue")
st.caption(
    "Pending candidates — below the topic-filter threshold but still "
    "captured for human review (per DECISIONS.md Q12). Promote turns a "
    "candidate into a kept study; reject is sticky across re-runs."
)

storage = storage_or_error()
if storage is None:
    st.stop()

# --- reviewer identity ----------------------------------------------------

default_reviewer = os.environ.get("USER", "operator")
with st.sidebar:
    reviewer = st.text_input("reviewer", value=default_reviewer)
    if not reviewer.strip():
        st.warning("set a reviewer name before promoting / rejecting")

# --- topic filter ---------------------------------------------------------

# Pull every distinct topic id present in the pending queue.
with storage.connection() as conn:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT unnest(topic_ids) AS t "
            "FROM study_scraper.studies WHERE status = 'pending' ORDER BY t"
        )
        pending_topics = [row["t"] for row in cur.fetchall()]

topic_filter: Optional[str] = st.selectbox(
    "topic",
    ["(all)"] + pending_topics,
    index=0,
)
if topic_filter == "(all)":
    topic_filter = None

limit = st.slider("how many", min_value=10, max_value=200, value=25, step=5)

rows = storage.list_studies(topic_id=topic_filter, status="pending", limit=limit)

if not rows:
    st.info(
        "No pending candidates"
        + (f" for topic `{topic_filter}`" if topic_filter else "")
        + ". Either the threshold is low enough that nothing is borderline, "
        "or every borderline candidate has already been reviewed."
    )
    st.stop()

st.write(f"showing {len(rows)} pending candidates")

# --- one row per candidate ------------------------------------------------

for row in rows:
    cid = row["id"]
    short = cid[:12]
    title = row.get("title") or ""
    abstract = row.get("abstract") or ""
    scores = row.get("topic_scores") or {}
    topic_ids = row.get("topic_ids") or []
    score_str = ", ".join(f"{k} = {v:.2f}" for k, v in scores.items()) or "—"
    url = row.get("canonical_url") or ""

    with st.expander(f"`{short}`  {title}", expanded=False):
        col_l, col_r = st.columns([3, 1])
        with col_l:
            st.markdown(f"**topics:** {', '.join(topic_ids)}  ·  **scores:** {score_str}")
            st.markdown(f"**source:** `{row.get('source_id')}`  ·  **language:** `{row.get('language') or '—'}`")
            st.markdown(f"**url:** {url}")
            if row.get("authors"):
                st.markdown("**authors:** " + ", ".join(row["authors"]))
            if abstract:
                st.markdown("**abstract**")
                st.write(abstract)
        with col_r:
            promote = st.button("promote → kept", key=f"prom_{cid}", type="primary")
            reject = st.button("reject", key=f"rej_{cid}")
            reason = st.text_input(
                "rejection reason (optional)", key=f"reas_{cid}", value=""
            )
            if promote:
                if not reviewer.strip():
                    st.warning("set a reviewer name first")
                else:
                    ok = storage.promote_study(cid, reviewed_by=reviewer.strip())
                    if ok:
                        st.success("promoted; reload to refresh the queue")
                    else:
                        st.error("no change (already kept?)")
            if reject:
                if not reviewer.strip():
                    st.warning("set a reviewer name first")
                else:
                    ok = storage.reject_study(
                        cid,
                        reviewed_by=reviewer.strip(),
                        reason=reason.strip() or None,
                    )
                    if ok:
                        st.success("rejected; reload to refresh the queue")
                    else:
                        st.error("no change (already rejected?)")

st.caption(
    "Reviewer decisions are sticky: re-running the scraper will not "
    "overturn them. The audit columns `reviewed_by` and `reviewed_at` "
    "are populated on every decision."
)
