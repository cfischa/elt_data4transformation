-- Migration 0011 — attribution_attempts: distinguish "tried, no signal"
-- from "never attempted" (resolves the mechanism flagged on #49).
--
-- `attribution_queue` (migration 0008) only excludes a study once it has
-- an `attributions` row. `upsert_attributions` never writes a row when
-- the LLM pass finds zero attributable triples for a study, so a
-- no-signal study never leaves the queue: it gets re-selected (and
-- re-billed against the fixed per-run batch) on every subsequent run,
-- forever, crowding out studies that haven't been tried yet.

SET search_path TO study_scraper, public;

CREATE TABLE IF NOT EXISTS attribution_attempts (
    study_id     CHAR(64)     NOT NULL REFERENCES studies(id) ON DELETE CASCADE,
    model        TEXT         NOT NULL,
    found        INT          NOT NULL,  -- triples yielded by this attempt
    attempted_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (study_id, model)
);

CREATE OR REPLACE VIEW attribution_queue AS
SELECT s.id, s.title, s.canonical_url, s.source_id, s.topic_ids
FROM   studies s
WHERE  s.status = 'kept'
  AND  EXISTS (SELECT 1 FROM claims c WHERE c.study_id = s.id)
  AND  NOT EXISTS (SELECT 1 FROM attributions a WHERE a.study_id = s.id)
  AND  NOT EXISTS (
        SELECT 1 FROM attribution_attempts aa
        WHERE aa.study_id = s.id AND aa.found = 0
       )
ORDER  BY s.fetched_at DESC;

INSERT INTO schema_versions (version, description)
    VALUES (11, 'attribution_attempts table; queue excludes no-signal attempts')
    ON CONFLICT (version) DO NOTHING;
