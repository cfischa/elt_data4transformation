"""Structured-data sources -- lake-style ingestion (Q16-v2).

These sources write to `source_records`. They are *not*
topic-filtered like `discovery/` sources: a DAWUM ingest pulls every
poll DAWUM publishes; topic association happens later, via views.

See docs/study_scraper/notes/structured-data-sources-2026-05-28.md for
the rationale and the per-source effort/yield table.
"""
