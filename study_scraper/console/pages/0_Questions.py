"""Questions — the product page: every registered question, answered.

THE page to show a PM: for each standing question in the registry
(config/topics/questions.yml) it runs the exact production answer path
(recall-aliased search over ALL attributions → population-aware
poll-of-polls aggregation) live against the DB and renders:

  * a one-glance summary table (question → headline answer + evidence),
  * per-question detail: every question-cluster with weighted %, spread,
    poll count, years, sample sizes, population — never a lone number,
  * coverage gaps (questions with no findings yet → collection work),
  * the latest digest shifts per watch (what changed since last run).

Read-only; the same numbers the scheduled runs publish in
question-answers.md / opinion-digest.md.
"""

from __future__ import annotations

import streamlit as st

from study_scraper.aggregate import aggregate_findings
from study_scraper.config import get_settings
from study_scraper.console._shared import storage_or_error
from study_scraper.questions import QuestionRegistryError, load_questions
from study_scraper.topics import load_topics


st.set_page_config(page_title="Study scraper — questions", layout="wide")
st.title("Questions — what does Germany think?")
st.caption(
    "Each registered question is answered from ALL relevant findings: "
    "recency- and sample-size-weighted poll-of-polls per question "
    "cluster, spread shown honestly, populations kept separate."
)

settings = get_settings()
try:
    topics = load_topics(settings.topics_csv_path)
    registry = load_questions(
        settings.questions_yaml_path,
        valid_topic_ids=[t.id for t in topics],
    )
except (QuestionRegistryError, FileNotFoundError) as exc:
    st.error("Question registry could not be loaded.")
    st.code(str(exc))
    st.stop()

topic_names = {t.id: t.primary_name for t in topics}

storage = storage_or_error()
if storage is None:
    st.stop()

# ---- answer every question with the production path -----------------------

results = []  # (question, [ClusterAnswer])
for q in sorted(registry, key=lambda q: (q.topic_id, q.id)):
    rows = storage.search_attributions_semantic(
        query=q.recall_query, limit=500, since=q.since_year
    )
    answers = aggregate_findings(rows)
    results.append((q, answers))

answered = [(q, a) for q, a in results if a]
gaps = [q for q, a in results if not a]

col1, col2, col3 = st.columns(3)
col1.metric("Registered questions", len(registry))
col2.metric("Answered from data", len(answered))
col3.metric(
    "Coverage gaps",
    len(gaps),
    delta=None if not gaps else "need collection",
    delta_color="off" if not gaps else "inverse",
)

# ---- one-glance summary table (the PM view) --------------------------------

summary_rows = []
for q, answers in answered:
    top = answers[0]                      # largest cluster = best evidence
    general = [p for p in top.positions if p.population == ""] or top.positions
    lead = max(general, key=lambda p: p.n_findings)
    summary_rows.append(
        {
            "Topic": topic_names.get(q.topic_id, q.topic_id),
            "Question": q.text,
            "Leading finding": (
                f"{lead.weighted_pct:.0f}% {lead.position}"
                + (f" (among {lead.population})" if lead.population else "")
            ),
            "Spread": (
                f"{lead.min_pct:.0f}–{lead.max_pct:.0f}%"
                if lead.n_findings > 1 else "single poll"
            ),
            "Polls": lead.n_findings,
            "Years": (
                "—" if lead.year_min is None
                else (
                    str(lead.year_max)
                    if lead.year_min == lead.year_max
                    else f"{lead.year_min}–{lead.year_max}"
                )
            ),
            "Clusters": len(answers),
        }
    )
if summary_rows:
    st.subheader("Answers at a glance")
    st.dataframe(summary_rows, use_container_width=True, hide_index=True)

if gaps:
    st.warning(
        "No findings yet for: "
        + "; ".join(f"**{q.text}** ({q.topic_id})" for q in gaps)
        + " — these need more collection (crawl + attribution), not code."
    )

# ---- per-question detail ----------------------------------------------------

st.subheader("Per-question detail")
for q, answers in results:
    head = topic_names.get(q.topic_id, q.topic_id)
    with st.expander(f"{head}: {q.text}", expanded=False):
        st.caption(
            f"recall terms: `{q.recall_query.replace('|', '` | `')}`"
            + (f" · since {q.since_year}" if q.since_year else "")
        )
        if not answers:
            st.info("Coverage gap — no findings matched yet.")
            continue
        for a in answers:
            st.markdown(f"**Q-cluster: {a.label}**  ·  {a.n_findings} finding(s)")
            detail = []
            for p in a.positions:
                detail.append(
                    {
                        "Position": p.position,
                        "Weighted %": round(p.weighted_pct, 1),
                        "Spread": (
                            f"{p.min_pct:.0f}–{p.max_pct:.0f}"
                            if p.n_findings > 1 else "—"
                        ),
                        "Polls": p.n_findings,
                        "Years": (
                            "—" if p.year_min is None
                            else (
                                str(p.year_max)
                                if p.year_min == p.year_max
                                else f"{p.year_min}–{p.year_max}"
                            )
                        ),
                        "Σ sample": (
                            f"{p.total_sample:,}" if p.total_sample else "—"
                        ),
                        "Population": p.population or "general",
                    }
                )
            st.dataframe(detail, use_container_width=True, hide_index=True)

# ---- tracking over time -----------------------------------------------------

st.subheader("Tracking over time")
watches = storage.list_watches()
tracked = []
for w in watches:
    snap = storage.latest_watch_snapshot(w["id"])
    if snap:
        tracked.append(
            {
                "Watch": w.get("label") or w["query"],
                "Last digest": str(snap["taken_at"])[:16],
                "Findings": snap["findings_count"],
                "Tracked cells": len(snap.get("payload") or []),
            }
        )
if tracked:
    st.dataframe(tracked, use_container_width=True, hide_index=True)
else:
    st.caption("No digest snapshots yet — the first scheduled digest creates them.")
st.caption(
    "≥5-point moves per (cluster, position, population) are flagged in "
    "each scheduled digest run (opinion-digest.md artifact) against the "
    "previous snapshot."
)
