-- Migration 0010 — mark registry-managed watches.
--
-- `questions sync` (config/topics/questions.yml) upserts watches by
-- query, but a registry EDIT changes the query string, which INSERTS a
-- new watch and strands the old one — still active, still answered by
-- every digest, double-counting the same polls (audit 2026-07-11).
-- `source` lets sync tell its own rows apart from manual `watch add`
-- rows, so it can deactivate stale registry watches without ever
-- touching a human's hand-registered question.

SET search_path TO study_scraper, public;

ALTER TABLE watches ADD COLUMN IF NOT EXISTS source TEXT;

INSERT INTO schema_versions (version, description)
    VALUES (10, 'watches.source: registry-managed vs manual')
    ON CONFLICT (version) DO NOTHING;
