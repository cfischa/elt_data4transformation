"""Candidate sources — unknown domains from kept studies (Phase 5d dock
surface, issue #38 / #77).

Reuses `domain_audit.audit_domains` verbatim: groups every kept study's
URL by registrable domain and lists the domains not already covered by
a dedicated source, ranked by frequency — candidates for the next
scraper to build.

Read-only.
"""

from __future__ import annotations

import streamlit as st

from study_scraper.console._shared import storage_or_error
from study_scraper.domain_audit import audit_domains


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
