"""Source kind classification used by `pages/5_Sources.py`.

Extracted so unit tests can exercise it without importing the Streamlit
page or `_shared.py` (which imports streamlit -- see `_csv.py` for the
same pattern).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from study_scraper.status import CATALOG_SOURCE_IDS, LAKE_SOURCE_IDS

# Known source kinds: "catalog" sources (study_scraper/discovery/*.py) write
# topic-filtered candidates to `studies`; "lake" sources
# (study_scraper/sources/*.py) write raw payloads to `source_records`
# (issue #123 -- this dict previously only covered the first 5 sources and
# silently misclassified everything added after it as "?"). The registry
# itself lives in `status.py` (single source of truth, also used to compute
# `never_run_sources`); kept in sync by
# tests/study_scraper/test_console.py::test_source_kind_covers_all_sources,
# which walks both packages and fails if a new source_id isn't listed there.
CATALOG_SOURCES = CATALOG_SOURCE_IDS
LAKE_SOURCES = LAKE_SOURCE_IDS


def source_kind(source_id: str) -> str:
    """Classify a source_id as 'catalog' (-> studies) or 'lake' (-> source_records).

    Returns '?' for an unrecognized source_id.
    """
    if source_id in CATALOG_SOURCES:
        return "catalog"
    if source_id in LAKE_SOURCES:
        return "lake"
    return "?"


def days_since(timestamp: Optional[datetime], *, now: Optional[datetime] = None) -> Optional[float]:
    """Days elapsed since `timestamp`, or None if `timestamp` is None.

    Same "days since last clean run" computation `status.py::build_status`
    does for the Home page's staleness table -- the Sources page's per-
    source table (`5_Sources.py`) previously only showed the raw "last
    successful run" timestamp, leaving an operator to do the date math by
    hand on the one page purpose-built for per-source health, while the
    Home page already surfaced the same signal as a sorted "days since"
    number. `now` is injectable for tests; defaults to the real current time.
    """
    if timestamp is None:
        return None
    if now is None:
        now = datetime.now(timezone.utc)
    return (now - timestamp).total_seconds() / 86400.0
