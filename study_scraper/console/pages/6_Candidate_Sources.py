"""Candidate sources & candidate studies (Phase 5d dock surface, issues
#38 / #77 / #141).

Two independent queues:

  - candidate **sources**: unknown domains from kept studies' URLs, via
    `domain_audit.audit_domains` — candidates for the next scraper to
    build.
  - candidate **studies**: OpenAlex work IDs referenced/related by
    studies we've already ingested but not yet ingested themselves, via
    `follow.pending_references` — candidates the reference-follower
    (#136) would fetch on its next `follow --fetch` run.

Read-only.
"""

from __future__ import annotations

import streamlit as st

from study_scraper.console._candidates import pending_reference_rows
from study_scraper.console._shared import storage_or_error
from study_scraper.domain_audit import audit_domains
from study_scraper.follow import pending_references


st.set_page_config(page_title="Study scraper — candidate sources", layout="wide")
st.title("Candidate sources — unknown domains")
st.caption(
    "Domains appearing in kept studies' URLs that no dedicated source "
    "covers yet, ranked by hit count. See `study_scraper sources-audit`."
)

storage = storage_or_error()
if storage is None:
    st.stop()

limit = st.slider("Max domains to show", min_value=5, max_value=100, value=20)
stats = audit_domains(storage, limit=limit)

if not stats:
    st.info("No unknown domains found.")
else:
    rows = [
        {"domain": s.domain, "hits": s.hits, "example url": s.example_url}
        for s in stats
    ]
    st.dataframe(rows, use_container_width=True, hide_index=True)

st.divider()
st.title("Candidate studies — reference follower")
st.caption(
    "OpenAlex work IDs referenced or related by studies we've already "
    "ingested, but not yet ingested themselves. Informational only — "
    "fetching them is a manual step, `study_scraper follow --fetch "
    "--topic <id>` (not scheduled; see issue #136)."
)

studies_limit = st.slider(
    "Max candidate studies to show", min_value=5, max_value=200, value=50,
)
pending_ids = pending_references(storage, limit=studies_limit)

if not pending_ids:
    st.info("No pending referenced works — nothing new since the last ingest.")
else:
    st.metric("pending candidate studies", len(pending_ids))
    st.dataframe(
        pending_reference_rows(pending_ids), use_container_width=True, hide_index=True,
    )
