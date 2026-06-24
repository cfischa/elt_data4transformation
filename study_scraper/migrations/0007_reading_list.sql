-- Migration 0007 — reading_list view (A20 hybrid model).
--
-- Maintainer 2026-05-31: "We could also think about having
-- quantitative statistics plus studies we need to read."
--
-- The split:
--   * quantitative track — studies whose claims table has rows
--     (regex-v1 over abstract and/or regex-v2 over full text);
--     queryable via `study_scraper search`.
--   * reading track — kept studies with NO extracted claims. Relevant
--     by topic, but the numbers (if any) haven't been machine-readable
--     yet. A human reads these; the dock / CLI surface them.
--
-- The `reason` column tells the operator why a study is on the list:
--   'no_artifact'  — full text not fetched yet (run `fulltext`)
--   'no_claims'    — full text fetched, still no numbers found:
--                    genuinely qualitative, or extraction failed.

SET search_path TO study_scraper, public;

-- Speeds up the fulltext queue query (kept studies missing artifact).
CREATE INDEX IF NOT EXISTS studies_artifact_pending_idx
    ON studies (fetched_at DESC)
    WHERE status = 'kept' AND raw_artifact_ref IS NULL;

CREATE OR REPLACE VIEW reading_list AS
SELECT
    s.id,
    s.title,
    s.canonical_url,
    s.source_id,
    s.publication_date,
    s.topic_ids,
    s.has_quantitative_data,
    s.raw_artifact_ref,
    CASE
        WHEN s.raw_artifact_ref IS NULL THEN 'no_artifact'
        ELSE 'no_claims'
    END AS reason
FROM   studies s
WHERE  s.status = 'kept'
  AND  NOT EXISTS (
           SELECT 1 FROM claims c WHERE c.study_id = s.id
       )
ORDER  BY s.publication_date DESC NULLS LAST;

INSERT INTO schema_versions (version, description)
    VALUES (7, 'reading_list view + fulltext queue partial index')
    ON CONFLICT (version) DO NOTHING;
