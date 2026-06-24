-- Migration 0004 — DOI as an alternate dedup key.
--
-- Same study can appear under different canonical_urls across sources
-- (the 2026-05-28 multi-topic run had the Bösche "Erdgas für den
-- Klimaschutz" record appear once as a SSOAR URL and once as an
-- openalex.org work id). With this column populated and indexed, the
-- storage layer's upsert path can detect "we already have this study
-- under a different canonical_url" and merge rather than insert.
--
-- The column is nullable: many SSOAR records and most older reports
-- don't carry a DOI. Title-near-duplicate (Phase 5b follow-on, lands
-- next iteration) handles those.

SET search_path TO study_scraper, public;

ALTER TABLE studies
    ADD COLUMN IF NOT EXISTS doi TEXT;

-- Partial index — DOI lookups are only meaningful when DOI is set, and
-- we keep the index small.
CREATE INDEX IF NOT EXISTS studies_doi_idx
    ON studies (doi)
    WHERE doi IS NOT NULL;

INSERT INTO schema_versions (version, description)
    VALUES (4, 'studies.doi: alternate dedup key for cross-source merge')
    ON CONFLICT (version) DO NOTHING;
