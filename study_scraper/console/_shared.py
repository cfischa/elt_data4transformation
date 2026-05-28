"""Helpers shared between Home.py and pages/*.py.

Kept minimal on purpose — the dock is two pages, not a framework.
"""

from __future__ import annotations

from typing import Optional

import streamlit as st

from study_scraper.config import get_settings
from study_scraper.storage import PostgresStorage, StorageError, resolve_database_url


def storage_or_error() -> Optional[PostgresStorage]:
    """Return a connected PostgresStorage, or render an error and stop.

    Returns None when the page should abort early.
    """
    settings = get_settings()
    try:
        url = resolve_database_url(
            postgres_url=settings.postgres_url,
            supabase_url=settings.supabase_url,
            supabase_service_key=settings.supabase_service_key,
        )
    except StorageError as exc:
        st.error("Database not configured.")
        st.code(str(exc))
        st.markdown(
            "Set `POSTGRES_URL` in your shell or `.env`, e.g.\n\n"
            "```bash\n"
            "export POSTGRES_URL=postgresql://postgres:postgres@localhost:5544/study_scraper\n"
            "```"
        )
        return None
    return PostgresStorage(url)
