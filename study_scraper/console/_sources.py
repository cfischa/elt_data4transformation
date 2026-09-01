"""Source kind classification used by `pages/5_Sources.py`.

Extracted so unit tests can exercise it without importing the Streamlit
page or `_shared.py` (which imports streamlit -- see `_csv.py` for the
same pattern).
"""

from __future__ import annotations


# Known source kinds: "catalog" sources (study_scraper/discovery/*.py) write
# topic-filtered candidates to `studies`; "lake" sources
# (study_scraper/sources/*.py) write raw payloads to `source_records`
# (issue #123 -- this dict previously only covered the first 5 sources and
# silently misclassified everything added after it as "?"). Kept in sync by
# tests/study_scraper/test_console.py::test_source_kind_covers_all_sources,
# which walks both packages and fails if a new source_id isn't listed here.
CATALOG_SOURCES = {"ssoar", "openalex", "bundestag_dip", "core"}
LAKE_SOURCES = {"dawum", "gesis", "eurostat", "govdata", "eurobarometer", "bmas"}


def source_kind(source_id: str) -> str:
    """Classify a source_id as 'catalog' (-> studies) or 'lake' (-> source_records).

    Returns '?' for an unrecognized source_id.
    """
    if source_id in CATALOG_SOURCES:
        return "catalog"
    if source_id in LAKE_SOURCES:
        return "lake"
    return "?"
