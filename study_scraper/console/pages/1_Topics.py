"""Topic-definition editor with live "what would match" preview.

Workflow per DECISIONS.md A11:
  1. The dock loads the topics CSV at `config/topics/topics.csv`.
  2. The human edits keywords for a topic.
  3. The "preview matches" panel shows how many studies *currently in
     the DB* would be kept under the edited topic, and lists the new
     hits + drops.
  4. On save, the dock writes the CSV back; the next CLI run picks the
     new definitions up. No ingest is triggered from the UI.

Out of scope on purpose: bulk import, branching, history, hosted
multi-user editing. This is an operator panel, not a CMS.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import streamlit as st

from study_scraper.config import get_settings
from study_scraper.console._csv import pipe_to_terms, terms_to_pipe, write_csv
from study_scraper.console._shared import storage_or_error
from study_scraper.topic_filter import score_text
from study_scraper.topics import Topic, TopicLocale, load_topics


st.set_page_config(page_title="Study scraper — topics", layout="wide")
st.title("Topics")
st.caption(
    "Edit topic definitions and preview which already-ingested studies "
    "would match. Saving writes back to `config/topics/topics.csv`."
)

settings = get_settings()
csv_path: Path = settings.topics_csv_path
st.code(str(csv_path), language=None)

topics = load_topics(csv_path)
topic_ids = [t.id for t in topics]

selected_id = st.selectbox(
    "Topic", topic_ids, index=0 if topic_ids else None
)
if not selected_id:
    st.info("No topics in CSV yet.")
    st.stop()
selected = next(t for t in topics if t.id == selected_id)

# ---- edit per-locale keywords --------------------------------------------

locales_sorted = sorted(selected.locales.keys())
locale_to_locale: Dict[str, TopicLocale] = {}
for lang in locales_sorted:
    st.subheader(f"Locale: {lang}")
    locale = selected.locales[lang]
    name = st.text_input(
        f"name ({lang})", value=locale.name, key=f"name_{lang}"
    )
    description = st.text_area(
        f"description ({lang})",
        value=locale.description,
        key=f"desc_{lang}",
        height=68,
    )
    include = st.text_area(
        f"include keywords ({lang}) — pipe-separated",
        value=terms_to_pipe(locale.include_keywords),
        key=f"inc_{lang}",
        height=68,
    )
    exclude = st.text_area(
        f"exclude keywords ({lang}) — pipe-separated",
        value=terms_to_pipe(locale.exclude_keywords),
        key=f"exc_{lang}",
        height=68,
    )
    synonyms = st.text_area(
        f"synonyms ({lang}) — pipe-separated",
        value=terms_to_pipe(locale.synonyms),
        key=f"syn_{lang}",
        height=68,
    )
    locale_to_locale[lang] = TopicLocale(
        name=name,
        description=description,
        include_keywords=pipe_to_terms(include),
        exclude_keywords=pipe_to_terms(exclude),
        synonyms=pipe_to_terms(synonyms),
    )

edited_topic = Topic(id=selected.id, locales=locale_to_locale)

# ---- live preview against the current DB ---------------------------------

st.subheader("Preview against currently-ingested studies")
st.caption(
    "Walks every row in `study_scraper.studies`, runs the rule-based "
    "topic filter against `title + abstract`, and shows what would "
    "match this topic at the chosen threshold."
)

threshold = st.slider("min score", 0.0, 1.0, value=0.2, step=0.05)

storage = storage_or_error()
if storage is None:
    st.stop()

# Fetch the full corpus (UI is operator-scoped, expect O(thousands) max
# during dev; we'll paginate when the DB outgrows this).
rows = storage.list_studies(limit=10_000)

current_hits: List[Dict[str, object]] = []
edited_hits: List[Dict[str, object]] = []
for row in rows:
    text = " ".join(filter(None, [row.get("title"), row.get("abstract")]))
    before = score_text(text, selected)
    after = score_text(text, edited_topic)
    if before.passes and before.score >= threshold:
        current_hits.append(
            {"score": round(before.score, 3), "title": row["title"], "url": row["canonical_url"]}
        )
    if after.passes and after.score >= threshold:
        edited_hits.append(
            {"score": round(after.score, 3), "title": row["title"], "url": row["canonical_url"]}
        )

current_set = {h["url"] for h in current_hits}
edited_set = {h["url"] for h in edited_hits}
added = [h for h in edited_hits if h["url"] not in current_set]
removed = [h for h in current_hits if h["url"] not in edited_set]

col_a, col_b, col_c = st.columns(3)
col_a.metric("Saved topic — hits", len(current_hits))
col_b.metric("Edited topic — hits", len(edited_hits))
col_c.metric(
    "Δ", f"{len(added) - len(removed):+d}",
    help=f"+{len(added)} new matches, −{len(removed)} dropped",
)

if added:
    st.markdown("**New matches under the edited topic**")
    for h in added:
        st.markdown(f"- `{h['score']}`  {h['title']}  \n  {h['url']}")
if removed:
    st.markdown("**No longer matched under the edited topic**")
    for h in removed:
        st.markdown(f"- `{h['score']}`  {h['title']}  \n  {h['url']}")
if not added and not removed and current_hits == edited_hits:
    st.info("No change against the saved topic.")

# ---- save ---------------------------------------------------------------

st.divider()
st.subheader("Save")
st.caption(
    "Writes `config/topics/topics.csv`. The next CLI run picks up the "
    "new definitions. This UI does not start an ingest."
)
if st.button("Save changes to topics.csv", type="primary"):
    new_topics = [t for t in topics if t.id != selected.id]
    new_topics.append(edited_topic)
    # Preserve original order: where edited_topic used to be.
    ordered: List[Topic] = []
    for t in topics:
        ordered.append(edited_topic if t.id == selected.id else t)
    write_csv(csv_path, ordered)
    st.success(f"Wrote {csv_path}. Reload the page to see persisted state.")
