# Decisions

This document tracks design decisions for the study scraper. Two sections:

- **Accepted** — decided, with rationale. Change requires a new entry that
  supersedes the previous one.
- **Open** — questions that block work. Each has a concrete proposal so the
  maintainer can reply with "yes" / "no, do X instead" / specifics.

---

## Accepted

### A1. Pivot from fixed-source ELT to study scraper
- **Date:** 2026-04-30
- **Decision:** Deprioritize the Airflow / ClickHouse / dbt / Streamlit ELT
  platform. Build a study scraper as the new core, driven by a topics sheet.
- **Rationale:** The repo's own `docs/PRD.md` already pointed at scraping the
  internet for representative polls; the build drifted into ELT plumbing.
  See `STATUS.md`.
- **Consequence:** Legacy code under `connectors/`, `dags/`, `dbt_project/`,
  `streamlit_app/` is not deleted but is **frozen**. New work goes into
  `study_scraper/` (proposed top-level package).

### A2. Independent project, minimal dependencies on legacy code
- **Date:** 2026-04-30
- **Decision:** The scraper must run without Airflow, ClickHouse, dbt, or
  Streamlit. Allowed cross-imports from legacy code: `config/topics/`
  (taxonomy YAML), and (with a thin abstraction) the matching rules from
  `pipeline/topic_classifier.py`.
- **Rationale:** Maintainer asked for "a truly independent coding project".
  Carrying the heavy stack would defeat the simplification.

### A3. Scrapy scaffold is broken — do not extend it
- **Date:** 2026-04-30
- **Decision:** `scraping/` will not be the home of new code. New scraping
  code lives under `study_scraper/`. We may either delete `scraping/` later
  or leave it frozen alongside the legacy ELT.
- **Rationale:** `scraping/settings.py` and `spiders/example_spider.py`
  reference modules (`pipelines`, `middlewares`, `items`, `utils`) that do
  not exist on disk. Fixing this in place buys nothing for the new project.

### A4. Topics sheet format = CSV (resolves Q1)
- **Date:** 2026-05-05
- **Decision:** Maintainer-edited topics live in
  `config/topics/topics.csv` with columns:
  `id, name, description, language, include_keywords, exclude_keywords,
  synonyms`. List-valued columns use `|` as separator (CSV-friendly,
  unambiguous). The existing `taxonomy.yml` is treated as a derived/legacy
  view; the scraper reads CSV directly.
- **Rationale:** Maintainer chose CSV in Q1.
- **Consequence:** Need a CSV loader (`study_scraper/topics.py`) and a
  bootstrap CSV seeded from the four topics in `taxonomy.yml`.

### A5. "Study" definition is inclusive (resolves Q3)
- **Date:** 2026-05-05
- **Decision:** A "study" is any artifact (HTML page or PDF) that is about
  at least one configured topic. Pure-qualitative policy papers without
  numbers are **in**. We capture them with a flag
  `has_quantitative_data: bool` (best-effort detection during extraction)
  so step 2 can filter if needed.
- **Rationale:** Maintainer chose "in" in Q3. Keeping a quantitative flag
  keeps the door open for step-2 filtering without losing recall now.

### A6. Tier-1 discovery sources = SSOAR, OpenAlex, DAWUM, Bundestag (resolves Q4)
- **Date:** 2026-05-05
- **Decision:** Build, in order: GESIS SSOAR → OpenAlex → DAWUM (reused as
  a source, not as the whole pipeline) → Bundestag publications. Tier 2
  (think tanks, polling release pages, BASE/CORE) is deferred until the
  eval harness exists.
- **Rationale:** Maintainer confirmed Q4.

### A7. Storage stack = Supabase, with a local-dev fallback (resolves Q5)
- **Date:** 2026-05-05
- **Decision:** Primary store is **Supabase** (Postgres for structured
  data, Supabase Storage for raw PDFs/HTML artifacts). Connection via
  `SUPABASE_URL` and `SUPABASE_SERVICE_KEY` env vars. ClickHouse remains
  available as a documented secondary sink **only if** Supabase is not
  reachable for a given deployment — but is not the default.
- **Local-dev fallback:** Code targets a clean SQL interface (asyncpg /
  psycopg) so a local Postgres (Docker container or `supabase start`) is
  drop-in equivalent. SQLite is **not** a fallback — Supabase-specific
  features (RLS, JSONB, full-text search) push us toward Postgres
  consistently.
- **Schema:** SQL migrations live under `study_scraper/migrations/` and
  are also valid Supabase migrations.
- **Rationale:** Maintainer chose Supabase in Q5. ClickHouse stays as the
  documented escape hatch per the wording "otherwise stay with
  ClickHouse".
- **Open sub-question:** Maintainer needs to provision a Supabase project
  and share `SUPABASE_URL` + service key (or set up a local
  `supabase start` instance) before live ingestion. See follow-up Q11.

### A8. Orchestration = CLI + cron (resolves Q6)
- **Date:** 2026-05-05
- **Decision:** No Airflow. The scraper is a Typer-based CLI exposed as
  `python -m study_scraper ...`. Periodic runs via cron locally or a
  GitHub Action when the project graduates to scheduled cloud runs.
- **Rationale:** Maintainer confirmed Q6.

### A9. Two-stage topic relevance: rules + local embeddings (resolves Q7)
- **Date:** 2026-05-05
- **Decision:** Stage 1 = rule-based matcher ported from
  `pipeline/topic_classifier.py` (cheap; runs over candidate titles +
  abstracts during discovery). Stage 2 = sentence-embedding similarity
  (multilingual model running locally; default
  `paraphrase-multilingual-MiniLM-L12-v2` or similar — exact pick recorded
  when integrated) over the abstract / first-page text. Both scores are
  stored on the `Study` row; final decision uses a tunable threshold per
  topic.
- **Rationale:** Maintainer accepted Q7. Local-only inference avoids API
  cost and external-call dependency.

### A21. LLM attribution extractor (Option A): structured (question, position, %) triples
- **Date:** 2026-06-15
- **Decision:** Add `llm-v1`, an Anthropic-API extractor that turns a
  study's text + regex claims into structured
  `(question, position, percentage)` attributions — the answer shape an
  issue-polling system needs. Maintainer chose **Option A** (a thin
  API-calling module) over Option B (a Cowork agent doing the
  reasoning).
- **Cost note (honest):** Option A meters tokens against the operator's
  `ANTHROPIC_API_KEY`. To honour the maintainer's "no extra API cost
  for now" intent, the same module also ships an **offline path**
  (`attribute-prompts` → answer in a Cowork session → `attribute-apply`)
  that runs the identical pure parser with zero API spend. Live and
  offline produce identical rows. Model defaults to `claude-opus-4-8`
  (skill default); `STUDY_SCRAPER_LLM_MODEL` overrides it for cheap bulk
  runs (e.g. `claude-haiku-4-5`) — the operator's cost call, not ours.
- **Implementation:** `study_scraper/extractors/llm_v1.py` (pure prompt
  build / response parse / JSON schema + lazy-SDK `extract_live`),
  `study_scraper/attribute.py` (orchestrator: live, dump-prompts,
  apply-responses), migration 0008 (`attributions` table +
  `attribution_queue` view), storage helpers, CLI `attribute` /
  `attribute-prompts` / `attribute-apply` / `ask`.
- **Design conformance (claude-api skill):** prompt caching on the
  frozen system prompt (per-study text is volatile, sent after the
  cached prefix); `output_config.format` structured output; lazy
  `import anthropic` so tests + offline path need neither SDK nor key.
- **Where it sits in the pipeline:** regex-v1 (abstract) and regex-v2
  (full text) find numbers; llm-v1 attributes them. Queue =
  `attribution_queue` (kept studies with claims, no attribution yet).
- **Still future:** dual-target attribution on `source_record`s (table
  supports it; orchestrator currently targets studies); confidence-
  weighted dedup of the same finding across studies.

### A20. Full-document statistics extraction unblocked (supersedes A13's deferral)
- **Date:** 2026-06-11
- **Decision:** Full-document scanning is now IN scope and built.
  Maintainer: "lots of relevant data scraped … does not mean to read
  just abstracts, rather than scanning a lot of documents, or even
  better, have all the statistics and quantitative connections for
  all the studies which are relevant. We could also think about
  having quantitative statistics plus studies we need to read."
- **What changed vs A13:** A13 deferred PDF extraction in favour of
  structured sources first. The structured-source tier-1 is now built
  (DAWUM, GESIS, Eurostat + SSOAR/OpenAlex catalog), so the deferral
  has served its purpose; A20 lifts it.
- **The hybrid model, as built:**
  1. **Quantitative track** — `study_scraper fulltext` fetches each
     kept study's document (PDF or HTML), extracts the FULL text, and
     runs the claim extractor over the whole body
     (`extractor='regex-v2'`, `source_field='fulltext'`). Coexists
     with abstract claims (`regex-v1`); each extractor's re-run
     replaces only its own rows.
  2. **Reading track** — kept studies with zero claims surface on the
     `reading_list` view (migration 0007) with a reason:
     `no_artifact` (fetch pending) or `no_claims` (fetched, but
     nothing quantitative — a human reads it).
- **Raw artifacts** are stored on disk under
  `settings.artifact_local_dir/<study_id>.<pdf|html>` and referenced
  via `studies.raw_artifact_ref` (provenance, re-extraction).
- **Still future:** LLM-based claim extractor (`llm-v1`) to
  disambiguate WHAT each percentage refers to; landing-page → PDF
  link resolution (waits for live hit-rate data; RUNBOOK §3); A17's
  crawl4ai stance is unchanged (re-evaluate with the T3 tier).

### A19. Eurostat lake source (additive)
- **Date:** 2026-05-31
- **Decision:** Add Eurostat as a lake source via the public
  dissemination API at
  `https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{code}`.
  No auth. Operator picks dataset codes per ingest run
  (`ingest --source eurostat --code env_air_gge --code nrg_bal_s`).
- **Rationale:** Per the A13 / A14 priority on structured-data
  sources, Eurostat is the lowest-friction T1 source after DAWUM and
  GESIS. JSON-stat 2.0 payloads preserve cleanly into `source_records`;
  per-table typed views (e.g. `eurostat_ghg`) can land later when an
  access pattern needs them.
- **Implementation:** `study_scraper/sources/eurostat.py`. Two modes
  (live + from_file), like DAWUM/GESIS. License recorded per record
  (CC BY 4.0). Fixture has real codes (`env_air_gge`, `nrg_bal_s`)
  in the JSON-stat 2.0 shape.

### A18. Phase 5d step 1 — capture OpenAlex citation graph in provenance
- **Date:** 2026-05-31
- **Decision:** When the OpenAlex source ingests a Work, capture its
  `referenced_works[]` and `related_works[]` arrays in two places:
  (a) `Candidate.raw` for downstream debugging; (b) `Study.provenance`
  (allowed-extras dict) so the IDs are queryable via SQL
  (`provenance->'referenced_works'`). Cap at 200 IDs per side to avoid
  bloating rows on long bibliographies.
- **Rationale:** Foundation for the Phase 5d reference-follower. The
  active fetcher (which materializes referenced IDs as new candidates)
  lands later; this commit makes the data available so the follower
  has somewhere to start.
- **No new schema:** uses the existing `provenance jsonb` column; no
  migration needed.

### A17. crawl4ai NOT adopted for the next iteration (evaluated 2026-05-31)
- **Date:** 2026-05-31
- **Decision:** Do not pull in
  [crawl4ai](https://github.com/unclecode/crawl4ai) for the next
  iteration. Re-evaluate when the A13 T3-tier (think-tank pages,
  polling-firm press releases) is unblocked.
- **Rationale:** crawl4ai is a real, mature tool (Python + Playwright
  + async; clean Markdown / JSON / PDF output; LLM-driven extraction
  optional). But:
  1. **Wrong tier:** our T1 sources (DAWUM, GESIS SPARQL, Destatis,
     Eurostat) all have structured APIs we already hit directly. No
     HTML rendering needed. crawl4ai would not improve them.
  2. **A13 deferred unstructured/PDF work** — adopting crawl4ai now
     would invite scope creep on the deferred tier.
  3. **A14 lake principle conflicts with LLM-pre-processed payloads**
     — we want raw payloads stored, not pre-summarised Markdown.
     crawl4ai's "clean Markdown" output filters content; the lake
     principle says preserve as-is, transform later via views.
  4. **Playwright weight:** Chromium runtime dependency adds 200+MB
     and complicates the operator environment.
- **When to reconsider:** when we wire the T3 tier (think-tank
  sitemaps, polling-firm press releases). crawl4ai then becomes a
  candidate alongside plain BeautifulSoup; pick by per-source need.

### A16. Title-near-duplicate dedup via pg_trgm (closes Phase 5b)
- **Date:** 2026-05-31
- **Decision:** When DOI dedup misses, fall back to a pg_trgm
  similarity lookup over `lower(title)`. Threshold 0.85, scoped to
  the same `publication_year` (or NULL on either side).
- **Rationale:** Many press-release-style records carry no DOI; the
  Erdgas-für-den-Klimaschutz study had been appearing twice in the
  catalog (once via SSOAR's handle URL, once via OpenAlex's openalex
  id). Title-near-dup collapses these without changing the data model.
- **Concrete shape (migration 0006):**
  - `CREATE EXTENSION pg_trgm WITH SCHEMA public` (explicit schema so
    `public.similarity` / `OPERATOR(public.%)` resolve regardless of
    session search_path).
  - GIN trigram index on `lower(title)`.
  - SQL function `find_title_dup(candidate_id, candidate_title,
    candidate_year, min_similarity=0.85)` returning the existing
    study to merge into, if any.
  - `upsert_study()` calls it as the fallback after DOI dedup.
- **Consequences:**
  - DOI dedup runs first; title dedup is the fallback. Both merge
    into the existing row; the new URL accumulates in `source_urls`
    and the new topic into `topic_ids`.
  - When the existing row has no DOI and the new candidate brings
    one, the DOI gets promoted into the existing row via COALESCE.
  - First-recorded `title` / `publisher` / `publication_date` /
    `abstract` stay frozen on dedup (lake principle).
  - Tests `test_dedup.py` cover identical-title, near-identical-
    title, different-titles, different-publication-year (no merge),
    DOI-before-title precedence, and DOI promotion on dedup.

### A15. GESIS Knowledge Graph SPARQL source — no auth (resolves Q17 catalog half)
- **Date:** 2026-05-31
- **Decision:** Ingest GESIS catalog metadata via the public SPARQL
  endpoint at `data.gesis.org/gesiskg/sparql`. **No authentication.**
  The legacy connector's `GESIS_API_KEY` env var was declared but
  never read; removed from `.env.example` and `docker-compose.yml`.
- **Rationale:** Q17 investigation 2026-05-29: the GESIS Knowledge
  Graph is public; the legacy `GESIS_API_KEY` slot was leftover from
  a different design intent. Same applies to `SOEP_API_KEY` (also
  unused; SOEP microdata uses account login, not an API key).
- **Implementation:** `study_scraper/sources/gesis.py` (lake source;
  emits `SourceRecord` per `schema:Dataset` URI; payload preserves
  the SPARQL SELECT result triples sorted for stable hashing). DOI is
  surfaced as a typed column when present in the triples.
- **Q17 microdata half (`search.gesis.org` post-login flow for
  SPSS/Stata/CSV bytes)** remains open as Q19 — deferred until we
  need bytes, not metadata.

### A14. Lake-style storage for structured-data sources (resolves Q16-v2)
- **Date:** 2026-05-29
- **Decision:** Structured-data sources land in **one universal
  `source_records` table** (not one table per source). Payload is
  captured as-is (`payload jsonb` for JSON/CSV/small structured,
  `payload_uri` for binary). Per-source typed access is provided via
  **SQL views over `source_records`**, added when an access pattern
  needs them — not pre-built per source.
- **Rationale:** Maintainer Q16-v2: "Collect the data as they are
  first. We can later think about how to process them." Pattern A
  (table per source) locks code to per-source schemas and forces a
  migration on every upstream change. Lake-then-view delays
  structural decisions until they pay off.
- **Concrete shape (migration 0005):**
  - `source_records (id, source_id, source_record_id, canonical_url,
     format, content_type, content_hash, payload jsonb, payload_uri,
     topic_ids, doi, license, fetched_at, discovery_run_id, status,
     reviewed_*, provenance jsonb, created_at, updated_at)`.
  - `id = sha256(source_id || '|' || canonical_url)` — idempotent
    across re-fetches, different across sources for the same URL.
  - CHECK constraint: exactly one of `payload` / `payload_uri` set.
  - Q12 review queue applies here too (`status` column).
- **`claims` table extended** with optional `source_record_id` FK and
  a CHECK that exactly one of `study_id` / `source_record_id` is set.
  Phase 6 claim extraction will write against either kind of target.
- **First view shipped (proof of pattern):** `dawum_polls` and
  `dawum_poll_results` over the DAWUM lake rows. Maintainer can run
  `SELECT party_shortcut, AVG(percentage) FROM
  study_scraper.dawum_poll_results GROUP BY party_shortcut` directly.
- **Q18 (file-only vs row-level) goes away** under this decision:
  everything is payload-stored; row-level access is a view, not an
  ingest-time choice. Q17 (GESIS auth) still open.
- **Supersedes:** Q16 (original "dedicated `datasets` table" proposal).

### A13. Structured data first; PDF full-text extraction deferred
- **Date:** 2026-05-28
- **Decision:** Prioritise sources that deliver **structured data**
  (databases, JSON / CSV / RDF / XLSX downloads, statistical-office APIs)
  over sources that publish unstructured prose (think-tank PDFs, news
  HTML). Phase 6-full (PDF text + claim re-extraction) is **future
  work**, not the next phase.
- **Rationale:** Maintainer 2026-05-28: "We should not focus on pdf
  study extraction first. Future task. We want to focus on database
  data / structured data like files and db." The 2026-05-28 example-
  question measurement showed the *mechanism* works (regex over
  abstracts produced 62 % / 55 % answers); what's missing is *volume*.
  Volume from structured sources comes cheaper, with better provenance,
  and without PDF-extraction quality risk.
- **Consequences:**
  1. **Sources expansion (Phase 5c) reorders** — structured sources
     first: DAWUM (JSON polling API), GESIS DBK (datasets + codebooks),
     Destatis GENESIS (statistical tables), Eurostat (statistical
     tables), BAMF migration data, UBA Klimabilanz / Umweltbundesamt
     structured downloads. Think-tank SitemapSource (HTML / PDF) drops
     in priority.
  2. **Polling-press-release source** is *not* what we build next —
     those publish HTML/PDF. The same questions are answerable from
     DAWUM (party polling) and from issue-poll datasets where they
     exist as structured files.
  3. **PDF fetching, OCR, full-text claim extraction** stay on the
     roadmap but explicitly **future work** — not before the
     structured sources land.
  4. **Schema impact** — a `datasets` notion is needed alongside
     `studies`. A study points at a publication; a dataset points at
     queryable rows. Q16 (new open question) below.

### A12. Coverage is the primary metric (paradigm sharpened)
- **Date:** 2026-05-28
- **Decision:** Coverage — the breadth of studies we have ingested
  across the universe of German-political-topic publications — is the
  **primary** project metric. Precision, extraction quality, and the
  other dimensions in `GOAL.md` are secondary and serve coverage being
  useful. When designs conflict, the choice that produces broader
  coverage wins by default.
- **Rationale:** Maintainer (2026-05-28): "Heart of the project is a
  high coverage of all studies for German politic topics." The
  original `GOAL.md` listed coverage as one of six dimensions; that
  framing implied equal weighting and led to a precision-leaning
  default at ingest time. The new framing matches the project's
  intent.
- **Consequences (changes to current code / docs):**
  1. `GOAL.md` rewritten: coverage promoted to primary; precision /
     extraction-quality reclassified as secondary. Studies containing
     extractable survey/poll data remain a **selection emphasis**
     (priority signal via `has_quantitative_data`, not a hard filter)
     — restored per maintainer 2026-05-28. Qualitative studies stay
     in scope per A5.
  2. Old success criteria (precision ≥ 90 %, fields ≥ 95 %) lowered to
     secondary thresholds. New primary criteria added: source count,
     studies-per-topic count, new-source discovery.
  3. The default ingest `min_score` stays a tunable knob, but the
     **default direction is recall over precision**. Precision is
     dock-managed (the topics editor's preview panel lets the operator
     fix false positives by editing keywords, not by raising the
     threshold).
  4. **No source dropped speculatively.** My earlier reasoning of
     "pull DAWUM only if a coverage gap shows up" is overturned by
     this decision — more sources = more coverage, period, unless a
     source is genuinely off-topic or paywalled.
  5. New work category: **new-source discovery** — tooling that finds
     candidate sources we haven't wired up yet. Becomes Phase 5d.

### A11. Streamlit allowed for the control UI only (scoped exception to A2)
- **Date:** 2026-05-28
- **Decision:** Streamlit is permitted **inside `study_scraper/console/`**
  and nowhere else. The ingest pipeline (`study_scraper/discovery/`,
  `pipeline.py`, `storage/`, CLI) must not import `streamlit` —
  enforced by code review, not by tooling.
- **Rationale:** A2 says the project must run without Streamlit. The
  control UI is a separate concern (read-only DB views + a CSV editor)
  with no path into the ingest pipeline; keeping it inside the same
  package is convenient and keeps it from drifting away from the data
  model. The cost of a tiny streamlit dependency for the operator UI
  is small; the cost of building a separate package for two pages is
  large.
- **Scope of the UI:** strictly two features for v1, both requested by
  the maintainer: (1) topics editor with live "what would match"
  preview, (2) coverage / status overview. No charts, no live ingest
  control, no auth — the dock runs locally against the operator's own
  Postgres. Anything beyond that needs a new decision.
- **Run command:** `streamlit run study_scraper/console/Home.py`. Not
  containerised. No legacy `streamlit_app/` reuse.

### A10. Eval gold set deferred (resolves Q8 partially)
- **Date:** 2026-05-05
- **Decision:** No gold set yet. Build the pipeline first; define the
  gold set after the first end-to-end run produces real candidates the
  maintainer can review and curate.
- **Rationale:** Maintainer answered "gold set later" in Q8.
- **Consequence:** Phase 5 (eval harness) is reordered to come **after**
  Phase 4 (first source end-to-end). The eval-driven success criteria in
  `GOAL.md` remain valid as the eventual bar — they just aren't measured
  until the gold set exists.

---

## Open questions

These are non-blocking. Each has a default that ships unless the
maintainer objects.

### Q2. Topic count for v1
**Default (in effect):** Start with the 4 topics already in
`taxonomy.yml` (`steuern`, `klima`, `migration_einwanderung`, `bildung`),
seeded into `topics.csv`. Maintainer can add rows any time.
**Override needed?** Only if you want a different starting set.

### Q9. Success-criteria thresholds
**Default (in effect):** As written in `GOAL.md` — ≥80% coverage on gold
set, ≥90% topic precision, ≥95% required-fields populated, ≥99% record-id
stability, on ≥3 topics.
**Override needed?** Only if you want different numbers. Becomes
measurable once Q8 (gold set) is resolved.

### Q10. Language scope
**Default (in effect):** German + English. Many German-society studies
are published in English (academic ones especially).
**Override needed?** Only if you want to add or drop languages.

### Q16. [SUPERSEDED by A14] How does a "dataset" sit alongside a "study"? (new, A13 follow-on)
Maintainer 2026-05-28 rejected the "table per source" pattern this
question was building toward: "Collect the data as they are first. We
can later think about how to process them." Resolved by A14
(lake-style `source_records` + views).

### Q16-v2. [RESOLVED by A14] Lake `source_records` design
**Context:** Structured sources (DAWUM, GENESIS, Eurostat) deliver
*data*, not just metadata + abstract. A DAWUM poll has columns
`(date, party, percentage, institute, n)`. A GENESIS table has rows
indexed by `(year, region, indicator)`. These don't fit the `studies`
shape — they're not papers.
**Proposal:** Add a `datasets` table that lives alongside `studies`:
  - `dataset` = a structured publication (poll, statistical table, codebook).
  - Same metadata as `studies` (id, canonical_url, source_id, topic_ids,
    title, publisher, publication_date, provenance) PLUS:
    - `format` text ('json' | 'csv' | 'xlsx' | 'rdf' | 'genesis-table' | …),
    - `schema_summary` jsonb describing columns / variables,
    - `row_count` int (when applicable),
    - `download_url` text (separate from canonical_url; where the bytes
      actually live).
  - `dataset_rows` table holds the actual rows for small / curated
    datasets where we want to query the data directly. Large datasets
    stay file-only and we just record the metadata.
  - `claims` table stays — claims now reference either a `study_id` OR
    a `dataset_id` via a discriminator column. Or, simpler: two claim
    tables, `study_claims` and `dataset_claims`.
**Need from you:** Endorse this shape, or send a different one.
(My rec: this shape; small fan-out, claims stays the answer surface.)

### Q11. Supabase provisioning (new — follow-up to A7)
**Default (in effect):** Code is being written behind a config interface
that works against either a hosted Supabase project or a local
`supabase start` / Postgres container. Until you provide credentials,
`make scrape` against live sources won't run end-to-end — but unit tests
and CLI smoke tests will.
**Need from you (when ready to ingest live):** Either (a) a Supabase
project URL + service-role key (placed in `.env`, not committed), or
(b) confirmation that you'll run `supabase start` locally for now.

---

## Decisions log conventions

- New decisions get the next `A<N>` id and append at the bottom of "Accepted".
- When superseding, link the old id and explain why.
- Open questions get the next `Q<N>` id. When answered, move to "Accepted"
  with the resolution and date.
