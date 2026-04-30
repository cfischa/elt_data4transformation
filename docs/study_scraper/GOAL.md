# Goal

## One-sentence goal

Build a high-class scraper that, given a topics list, discovers and extracts
relevant **studies** (representative surveys, polls, and research publications
containing quantitative data about German society) from the public web, into a
structured store suitable for downstream data engineering.

## Two-step plan (per maintainer)

1. **Now — Scraping (this project):** find and extract relevant studies for
   each topic into a structured, deduplicated, queryable store.
2. **Later — Data engineering:** use the extracted data to answer policy-level
   questions of the form *"what does German society want on topic X?"*.

This document is about step 1. Step 2 informs the schema (we need to capture
fields that step 2 will need) but is **not implemented here**.

## What "high-class" means here

A working definition we can hold ourselves to:

1. **Coverage** — for each configured topic, the scraper surfaces a high
   fraction of the publicly-available, recent (last N years), German-context
   studies that contain extractable survey/poll data.
2. **Precision** — when the scraper claims a study is about a topic, it is
   actually about that topic. Few false positives.
3. **Extraction quality** — for each study we capture at least:
   `title, authors, publisher, publication_date, source_url, language,
   abstract, topic_ids, key_findings (where extractable), survey_metadata
   (sample size, fieldwork dates, methodology) where present, and the
   raw artifact (PDF/HTML)`.
4. **Provenance** — every record has a verifiable trail back to the source URL
   and a fetch timestamp. No silent transformations.
5. **Reproducibility** — re-running the scraper on the same inputs yields
   the same set of records (modulo new publications).
6. **Iteration-friendly** — there is an eval harness with a small gold set,
   so we can measure precision/recall/extraction quality each run and tune.

## Success criteria (proposed — to be ratified)

The project reaches its goal when, for at least **3 topics from the topics
sheet**, on a frozen evaluation date:

- [ ] Coverage ≥ 80% on a maintainer-curated gold set of ≥ 20 known studies
      per topic.
- [ ] Topic-classification precision ≥ 90% on a sampled review of 50 records.
- [ ] Required fields populated for ≥ 95% of records (`title`, `source_url`,
      `publication_date`, `topic_ids`, `language`); survey metadata populated
      where the study contains it (best effort; subject to extractor maturity).
- [ ] Re-running yields a stable record set: ≥ 99% record-id stability across
      back-to-back runs.
- [ ] Eval harness produces a single-page report (markdown) showing the above
      metrics — checked into the repo per run.

These thresholds are **proposals** in `DECISIONS.md` Q9 — confirm or adjust
before we treat them as the bar.

## Scope

### In scope

- Discovery from public sources (open academic indexes, government
  publications, think-tank publications, polling aggregator pages).
- HTML and PDF extraction.
- Topic-driven filtering (rule-based + optionally semantic; see Q3).
- Deduplication across sources.
- Structured output with provenance.
- An eval harness and a small gold set.
- A topics sheet that a non-developer can edit.

### Out of scope (for now)

- Live dashboards / Streamlit UI (legacy `streamlit_app/` is not part of this
  project).
- Star-schema analytics, dbt models, ClickHouse — unless an accepted decision
  reverses this. SQLite/DuckDB are the v1 default.
- Airflow orchestration. Cron + a CLI is the v1 default.
- Paywalled or login-gated content.
- Social media scraping (legal/ToS friction; reconsider later if needed).
- Step 2 ("what does German society want") modeling and aggregation.

## Non-goals (explicit)

- We are **not** rebuilding the ELT platform.
- We are **not** trying to be a general-purpose web crawler. Discovery is
  narrow: per topic, per source, with quality filters.
- We are **not** chasing exhaustive historical coverage on day 1. Recency
  window first; backfill later if useful.
