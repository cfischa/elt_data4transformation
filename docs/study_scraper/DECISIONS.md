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
  or leave it frozen alongside the legacy ELT. Decision deferred to A4.
- **Rationale:** `scraping/settings.py` and `spiders/example_spider.py`
  reference modules (`pipelines`, `middlewares`, `items`, `utils`) that do
  not exist on disk. Fixing this in place buys nothing for the new project.

---

## Open questions (block first implementation)

> Format: each Q has **(a) a concrete proposal** and **(b) what I need from
> you**. Reply with the Q number and either "yes" / "no, X instead" /
> specifics.

### Q1. What is the topics sheet format?
**Proposal:** Keep `config/topics/taxonomy.yml` as the source of truth, but
add a thin loader so the scraper can also read a CSV at
`config/topics/topics.csv` with columns `id,name,description,
include_keywords,exclude_keywords,language`. The CSV is the
human-editable surface; a converter generates/updates the YAML.
**Need from you:** Confirm CSV-as-edit-surface, or prefer Google Sheets
sync, or stick to YAML only? If Google Sheets: do you want a one-way pull
into the repo (committed snapshot) or a runtime fetch?

### Q2. How big is the topic list, day 1?
**Proposal:** Start with the 4 topics already in `taxonomy.yml`
(`steuern`, `klima`, `migration_einwanderung`, `bildung`) plus 2–4 you add
during the next iteration. Optimize for those, not for 50.
**Need from you:** Confirm 4–8 topics for v1, and name any topics you want
added now.

### Q3. What counts as a "study" worth keeping?
**Proposal:** A study is an artifact (HTML page or PDF) that:
1. Is about at least one configured topic (passes the topic filter), **and**
2. Contains or summarizes representative quantitative data about German
   society (polls, surveys, official statistics with methodology notes).
"Contains" is a weaker test than "extractable now"; we capture the artifact
+ best-effort metadata even if we can't parse the numbers on day 1.
**Need from you:** Confirm. Specifically: do you want pure-qualitative
policy papers (no numbers) excluded, included with a flag, or included
without distinction?

### Q4. Which discovery sources do we target first?
**Proposal — tier 1 (build first, in this order):**
1. **GESIS SSOAR** — `https://www.ssoar.info` open social science repo;
   already partially understood by the legacy GESIS connector.
2. **OpenAlex** — `https://api.openalex.org` free academic graph, REST API,
   strong filtering by topic + year + language.
3. **DAWUM polls aggregator** — `https://dawum.de` already has a connector;
   reuse it as one source rather than the whole pipeline.
4. **Bundestag publications search** — `https://www.bundestag.de` for
   published studies and Sachstandsberichte commissioned by the parliament.

**Proposal — tier 2 (after tier 1 works end-to-end):**
- Think tank sitemaps/RSS: Bertelsmann Stiftung, DIW Berlin, Ifo, Konrad-
  Adenauer-Stiftung, Friedrich-Ebert-Stiftung, Sachverständigenrat.
- Polling release pages: Allensbach, Forsa, Infratest dimap (where
  permissible).
- BASE / CORE academic search.

**Proposal — explicitly deferred:** general news scraping (legal/ToS
friction), social media (out of scope per `GOAL.md`).

**Need from you:** Confirm tier 1, edit tier 2, flag any source that's
either critical for v1 or that you want excluded for legal/political reasons.

### Q5. Storage stack for v1
**Proposal:** **DuckDB** as the primary store (file-based, zero-ops, fast
analytical queries, plays nicely with Pandas/Polars). Raw artifacts (PDFs,
HTML snapshots) on the local filesystem under `data/study_scraper/raw/`,
referenced by hash. Defer ClickHouse to v2 if a real need shows up.
**Need from you:** OK to drop ClickHouse for v1? Or do you want the scraper
to write into the existing ClickHouse instance from day 1?

### Q6. Orchestration for v1
**Proposal:** A Python CLI (`python -m study_scraper run --topic steuern`)
plus a `Makefile` target. No Airflow. Periodic runs via cron or a GitHub
Action when ready.
**Need from you:** Confirm. Or do you want to keep Airflow because the
container is already running?

### Q7. Topic relevance: rules + semantic, or rules only?
**Proposal:** Two-stage. Stage 1 = the existing rule-based matcher
(cheap, transparent, used during discovery to filter candidate URLs and
titles). Stage 2 = a semantic relevance score using sentence embeddings
(applied to abstract / first-page text after fetch) — needed because many
real studies use vocabulary that doesn't literally match the taxonomy
keywords.
**Need from you:** OK to add a small embeddings model (e.g. multilingual
`paraphrase-multilingual-MiniLM` or similar)? Local-only inference, no
external API calls. If you'd rather keep v1 rules-only and add embeddings
in v2, say so.

### Q8. Eval harness — what does "the goal is reached" mean concretely?
**Proposal:** Maintain a small **gold set** at
`docs/study_scraper/eval/gold/<topic_id>.yml` listing 20+ known studies per
topic with expected metadata. The scraper's eval mode runs against these
and emits a report (`eval/report-YYYY-MM-DD.md`) with coverage, precision,
extraction-field completeness. The success criteria in `GOAL.md` are the
acceptance bar.
**Need from you:** Will you supply the initial gold set (you know the field;
I don't)? Or do you want me to bootstrap one from each tier-1 source's
high-confidence matches and have you review?

### Q9. Are the success-criteria thresholds in GOAL.md correct?
**Proposal:** As written: ≥80% coverage on gold set, ≥90% topic precision,
≥95% required-fields populated, ≥99% record-id stability, on at least 3
topics.
**Need from you:** Adjust thresholds, or confirm. These drive when we say
"done".

### Q10. Language scope
**Proposal:** German + English only for v1. Many German-society studies are
published in English (especially academic ones).
**Need from you:** Confirm. Add or drop languages.

---

## Decisions log conventions

- New decisions get the next `A<N>` id and append at the bottom of "Accepted".
- When superseding, link the old id and explain why.
- Open questions get the next `Q<N>` id. When answered, move to "Accepted"
  with the resolution and date.
