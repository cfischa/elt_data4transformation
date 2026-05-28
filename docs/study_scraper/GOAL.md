# Goal

## One-sentence goal

Build a study scraper whose **primary metric is coverage**: discover and
ingest as broad a set as possible of the publicly-available studies,
reports, and surveys about German political topics, into a structured
store suitable for downstream data engineering.

> **Heart of the project (added 2026-05-28, per maintainer):** the
> point of this scraper is *coverage* — getting an overview of what
> exists. Precision, extraction quality, and other dimensions are
> secondary and exist in service of coverage being useful to a human
> reviewer. When two design choices conflict, the one that produces
> broader coverage wins by default.

## Two-step plan (per maintainer)

1. **Now — Scraping (this project):** find and ingest the broad
   universe of German-political-topic studies into a structured,
   deduplicated, queryable store. Coverage first. Human-in-the-loop
   curation (via the control dock) handles edge cases.
2. **Later — Data engineering:** use the extracted data to answer
   policy-level questions of the form *"what does German society want
   on topic X?"*.

This document is about step 1. Step 2 informs the schema (we need to
capture fields that step 2 will need) but is **not implemented here**.

## What "high coverage" means here

The dimensions in priority order. Higher dimensions outrank lower ones
when designs conflict.

1. **Coverage (PRIMARY).**
   For each configured topic, the scraper surfaces a high fraction of
   the publicly-available German-context studies on that topic —
   academic, government, think-tank, polling aggregator, civic-society.
   Not restricted to studies with quantitative data (per A5, qualitative
   policy papers are kept too with a flag).

   Coverage has two faces:
   - **Source coverage** — how many of the publishers / repositories
     that publish on a topic do we have ingestion for? (Many sources,
     not many studies from one source.)
   - **Study coverage within a source** — given a source, what fraction
     of its relevant studies have we ingested?

2. **Provenance.**
   Every record has a verifiable trail back to its source URL, fetch
   timestamp, and discovery method. Required because the data
   downstream is only as trustworthy as its origin trail.

3. **Reproducibility.**
   Re-running on the same inputs yields the same set of records.
   Required so the human review of borderline records doesn't have to
   start over on every run.

4. **Iteration-friendly.**
   The control dock + the topics CSV are the operator's feedback loop.
   A human edits a topic, immediately sees the impact on already-
   ingested studies, and re-runs ingest with the new definition.

5. **Extraction quality (secondary).**
   For each study we *try* to capture title, authors, publisher,
   publication_date, source_url, language, abstract, topic_ids,
   key_findings, survey_metadata where present, and a raw-artifact ref.
   "Try" is the word: missing fields are recorded as NULL and do not
   block ingestion. Phase 6 (extractor maturity) tightens this without
   gating coverage.

6. **Precision (secondary).**
   Topic precision is achieved by **human review in the dock**, not by
   tight automated thresholds at ingest time. The default ingest
   threshold is intentionally low; the dock makes it cheap to spot and
   triage false positives. We do not throw away candidates to chase a
   precision number.

## What changed in this rewrite (2026-05-28)

- Coverage promoted from "one of six dimensions" to **the primary
  metric**.
- The narrow framing "studies that contain extractable survey/poll
  data" is removed. Per A5, any study about a configured topic is in
  scope; the `has_quantitative_data` flag is the marker, not a gate.
- Precision and extraction quality re-classified as **secondary** — they
  serve coverage being useful, but a precision deficit is fixed in the
  dock, not by tightening ingest.
- The non-goal "not exhaustive historical coverage on day 1" stays —
  we still prefer recency — but is now clearly a sequencing choice,
  not a scope reduction.

## Success criteria

Revised for the coverage-first framing. Old (precision-heavy) thresholds
move to "secondary".

### Primary

The project reaches its goal when, on a frozen evaluation date:

- [ ] **Source coverage**: at least **8 production sources** wired up,
      spanning academic / government / think-tank / polling categories.
      "Production" means a discovery source class with at least one
      successful CLI run against a topic.
- [ ] **Topic coverage**: for at least **3 topics from the topics
      sheet**, the DB contains ≥ 50 distinct studies each, drawn from
      ≥ 3 of those sources.
- [ ] **Recall on the gold set**: ≥ 80 % coverage on a maintainer-
      curated gold set of ≥ 20 known studies per topic.
- [ ] **New-sources discovery**: a tool / process that surfaces
      candidate *new* sources beyond the standard list, with at least
      5 new sources reviewed and accepted/rejected by a human reviewer.

### Secondary

- [ ] Topic precision on the kept set ≥ 70 % on a sampled human review
      (lowered from 90 %; precision is dock-managed).
- [ ] Required fields populated for ≥ 90 % of records (`title`,
      `source_url`, `publication_date`, `topic_ids`); survey metadata
      best-effort.
- [ ] Re-runs are stable (≥ 99 % record-id stability), modulo the
      explicit `source_urls` merge on re-discovery.

## Scope

### In scope

- Discovery from public sources: open academic indexes (OpenAlex, BASE,
  CORE), social-science repositories (SSOAR, GESIS DBK), government
  publications (Bundestag DIP, Umweltbundesamt, BAMF, BMAS, …),
  think-tank publications (SWP, DIW, Ifo, Bertelsmann, FES, KAS,
  Sachverständigenrat, …), polling aggregators (DAWUM,
  wahlrecht.de, …).
- HTML and PDF extraction.
- Topic-driven filtering (rule-based + optionally semantic; see Q3).
- Deduplication across sources (canonical-URL hash + DOI + title
  near-duplicate).
- **New-source discovery**: tooling that surfaces studies / domains we
  haven't ingested yet (citation following, related-works expansion,
  domain audit against web search). The output is a queue of *candidate
  new sources* a human reviews in the dock.
- Structured output with provenance.
- An eval harness and a small gold set.
- A topics sheet that a non-developer can edit.

### Out of scope (for now)

- Live dashboards / Streamlit UI **beyond** the operator dock at
  `study_scraper/console/` (per A11 — operator tooling only).
- Star-schema analytics, dbt models, ClickHouse. Postgres / Supabase
  per A7.
- Airflow orchestration. Cron + a CLI is the v1 default.
- Paywalled or login-gated content.
- Social media scraping.
- Step 2 ("what does German society want") modeling and aggregation.

## Non-goals (explicit)

- We are **not** rebuilding the ELT platform.
- We are **not** a general-purpose web crawler. Discovery is
  topic-driven; new-source discovery is human-reviewed.
- We are **not** chasing exhaustive historical coverage on day 1.
  Recency window first; backfill later if useful.
- We are **not** optimising for precision at ingest time. Precision is
  a dock concern.
