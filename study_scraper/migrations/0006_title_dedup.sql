-- Migration 0006 — title-near-duplicate dedup (closes Phase 5b).
--
-- DOI dedup (migration 0004) handles cross-source duplicates whenever
-- both sources surface a DOI. Older / press-release-style records
-- often have no DOI; those slip through. pg_trgm gives us cheap
-- in-database similarity matching for those cases.
--
-- We add:
--   - pg_trgm extension (Postgres built-in; standard);
--   - a GIN trigram index on lower(title) for fast similarity lookup;
--   - a helper function `find_title_dup(...)` that returns the
--     existing study id whose title is sufficiently similar to a
--     given title AND whose publication_year matches.
--
-- The application layer (storage.upsert_study) calls the function as
-- a fallback after DOI dedup fails -- it's intentionally side-effect
-- free and small.

-- Install pg_trgm explicitly into the `public` schema so the function
-- body below can reference `public.similarity` / `OPERATOR(public.%)`
-- regardless of session search_path. Without the explicit schema,
-- pg_trgm gets created in whatever the first schema on the search path
-- happens to be at install time (often `study_scraper`).
CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;

SET search_path TO study_scraper, public;

CREATE INDEX IF NOT EXISTS studies_title_trgm_idx
    ON studies USING GIN (lower(title) public.gin_trgm_ops);

-- The function body references `public.similarity` and the `%`
-- operator that ship with pg_trgm. In the same transaction as
-- CREATE EXTENSION, Postgres's function-body validator can't see the
-- newly-created extension's functions yet. Disable the validator for
-- this CREATE; the function still runs correctly at call time when
-- the extension is fully committed.
SET LOCAL check_function_bodies = off;

-- Look up an existing study by title near-duplicate.
--   - Similarity threshold defaults to 0.85 (matches 'verbindliches
--     Klimaschutzgesetz' to 'verbindliche Klimaschutzgesetze',
--     rejects 'Klimaschutz in Kommunen' vs 'Klimaschutz für
--     Energiewende').
--   - publication_year must match when both rows have it; either-
--     side NULL is permissive.
--   - Excludes the candidate's own id (so upsert by id stays separate
--     from cross-source dedup).
--   - Status='rejected' rows are NOT excluded -- a human's rejection
--     sticks. The caller decides what to do if dedup hits a rejected
--     row.
CREATE OR REPLACE FUNCTION find_title_dup(
    candidate_id      TEXT,
    candidate_title   TEXT,
    candidate_year    INTEGER,
    min_similarity    NUMERIC DEFAULT 0.85
)
RETURNS TABLE(id CHAR(64), title TEXT, sim NUMERIC, status TEXT) AS $$
    SELECT s.id, s.title,
           public.similarity(lower(s.title), lower(candidate_title))::numeric AS sim,
           s.status
    FROM   study_scraper.studies s
    WHERE  s.id <> candidate_id
      AND  lower(s.title) OPERATOR(public.%) lower(candidate_title)
      AND  public.similarity(lower(s.title), lower(candidate_title)) >= min_similarity
      AND  (
              candidate_year IS NULL
           OR EXTRACT(YEAR FROM s.publication_date) IS NULL
           OR EXTRACT(YEAR FROM s.publication_date) = candidate_year
           )
    ORDER  BY sim DESC, s.created_at ASC
    LIMIT  1;
$$ LANGUAGE SQL STABLE;

INSERT INTO schema_versions (version, description)
    VALUES (6, 'pg_trgm title dedup: find_title_dup() + GIN index')
    ON CONFLICT (version) DO NOTHING;
