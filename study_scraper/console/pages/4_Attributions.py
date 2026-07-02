"""Attributions browser — the answer layer (A21).

Browse and filter the structured `(question, position, percentage)`
triples the llm-v1 extractor produced from study claims. This is the
human view of "what does German society think about X": filter by topic,
position, or free text; sort by percentage; click through to the source.

Read-only. Lake (source_record) attributions are not shown here yet —
this page joins study-backed attributions only.
"""

from __future__ import annotations

import streamlit as st

from study_scraper.console._shared import storage_or_error


st.set_page_config(page_title="Study scraper — attributions", layout="wide")
st.title("Attributions — structured findings")
st.caption(
    "Each row is a (question, position, percentage) triple extracted by "
    "`llm-v1` from a study's claims. Filter and sort to answer "
    "'what share supports / opposes X?'."
)

storage = storage_or_error()
if storage is None:
    st.stop()

total = storage.count_attributions()
st.metric("attributions stored", total)
if total == 0:
    st.info(
        "No attributions yet. Run the attribution pass: "
        "`python -m study_scraper attribute` (live) or the offline "
        "`attribute-prompts` → `attribute-apply` path."
    )
    st.stop()

topics = storage.list_distinct_attribution_topics()

col_a, col_b, col_c = st.columns([2, 1, 1])
with col_a:
    query = st.text_input("search question text", value="")
with col_b:
    position = st.selectbox(
        "position", ["(all)", "support", "oppose", "neutral", "unspecified"]
    )
with col_c:
    topic = st.selectbox("topic", ["(all)"] + topics)

col_d, col_e = st.columns([1, 3])
with col_d:
    ungrounded_only = st.checkbox(
        "ungrounded only",
        value=False,
        help="Show only findings whose source_span did NOT verify against "
             "the source text — the likely-hallucination review queue.",
    )

limit = st.slider("limit", min_value=10, max_value=500, value=100, step=10)

rows = storage.filter_attributions(
    query=query or None,
    position=None if position == "(all)" else position,
    topic=None if topic == "(all)" else topic,
    limit=limit,
)

if ungrounded_only:
    rows = [r for r in rows if (r.get("raw") or {}).get("grounded") is False]

st.write(f"showing {len(rows)} finding(s)")
if not rows:
    st.info("No attributions match the current filters.")
    st.stop()


def _grounded_badge(raw: dict) -> str:
    g = raw.get("grounded")
    if g is True:
        return "✓"
    if g is False:
        return "✗"
    return "—"  # not checked (no source text at parse time)


# Compact table view with the trust signals (source_span evidence,
# grounded badge, distribution warning) so accuracy is auditable by eye.
table = []
for r in rows:
    raw = r.get("raw") or {}
    table.append({
        "%": (f"{float(r['percentage']):.0f}" if r.get("percentage") is not None else "—"),
        "position": r.get("position"),
        "question": r.get("question"),
        "grounded": _grounded_badge(raw),
        "⚠": "⚠" if raw.get("distribution_check") is False else "",
        "evidence (source_span)": raw.get("source_span") or "",
        "population": r.get("population") or "",
        "confidence": (f"{float(r['confidence']):.2f}" if r.get("confidence") is not None else ""),
        "dups": r.get("dup_count") or "",
        "source": r.get("source_id"),
        "study": r.get("title"),
        "url": r.get("canonical_url"),
    })
st.dataframe(
    table,
    use_container_width=True,
    hide_index=True,
    column_config={
        "url": st.column_config.LinkColumn("url"),
        "grounded": st.column_config.TextColumn(
            "grounded",
            help="✓ span verified in source · ✗ NOT found (likely "
                 "hallucination, confidence capped) · — not checked",
        ),
        "⚠": st.column_config.TextColumn(
            "⚠", help="positions for this question sum to >120%",
        ),
    },
)

st.caption(
    "Tip: the same finding can appear across multiple studies. "
    "Cross-study dedup (confidence-weighted) is a separate read-time view."
)
