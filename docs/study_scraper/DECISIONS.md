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
