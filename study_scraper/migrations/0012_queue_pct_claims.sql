-- Migration 0012 — attribution_queue exposes `pct_claims`.
--
-- A (question, position, percentage) triple needs a percentage in the
-- source text. Studies whose regex claims contain no '%' unit claim
-- almost never yield one, yet they fill batch slots (issue #196: yield
-- 1/40 with a ~500 backlog). The orchestrator uses this count to try
-- percent-bearing studies first. Column is appended last so
-- CREATE OR REPLACE VIEW stays valid.

SET search_path TO study_scraper, public;

CREATE OR REPLACE VIEW attribution_queue AS
SELECT s.id, s.title, s.canonical_url, s.source_id, s.topic_ids,
       (SELECT COUNT(*) FROM claims c
         WHERE c.study_id = s.id AND c.unit = '%') AS pct_claims
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
    VALUES (12, 'attribution_queue.pct_claims column')
    ON CONFLICT (version) DO NOTHING;
