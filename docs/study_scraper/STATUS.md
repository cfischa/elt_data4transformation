# Status — independent analysis of the repo

_Last updated: 2026-05-31 (clean+build pass: scrapy scaffold deleted,
dead env vars removed, GESIS catalog source shipped (A15), title-near-
dup dedup shipped closing Phase 5b (A16), status report enhanced with
lake counters, crawl4ai evaluated and declined for now (A17).
**183 tests pass.**)._

## Clean + build pass (2026-05-31)

Cleanup:
- Deleted `scraping/` (broken Scrapy scaffold, never extended per A3).
- Removed `GESIS_API_KEY` / `SOEP_API_KEY` from `.env.example` and
  `docker-compose.yml` (dead config: declared but never read).
- Removed `scrapy` and `selenium` from `pyproject.toml`; left `bs4`
  for the legacy ELT.
- Added `SPARQLWrapper` for the GESIS catalog source.
- Root `README.md` rewritten to point at `study_scraper/` as the
  active project; legacy ELT clearly labelled frozen.

Build:
- **GESIS Knowledge Graph SPARQL source** (A15) —
  `study_scraper/sources/gesis.py`. Public endpoint, no auth. Lake-
  style; emits one `SourceRecord` per `schema:Dataset` URI with
  payload preserving sorted triples. CLI: `python -m study_scraper
  ingest --source gesis [...]`. Fixture has real ZA-numbers + DOIs
  (Politbarometer 1977-2024 ZA2391, UBA Umweltbewusstsein 2022
  ZA8829, UBA Umweltbewusstsein 2018 ZA7493, + negative control).
- **Title-near-duplicate dedup (Phase 5b finished)** (A16) —
  migration 0006 + `find_title_dup()` Postgres function + fallback
  pass in `upsert_study()`. The Erdgas study now correctly collapses
  to one row across SSOAR + OpenAlex.
- **Status report enhanced** with lake counters: total source_records
  kept, per-source breakdown, per-format breakdown.

Cleanly running end-to-end ingest across **four sources**:

  $ python -m study_scraper run    --source ssoar    --topic klima
  $ python -m study_scraper run    --source openalex --topic klima
  $ python -m study_scraper ingest --source dawum    --topic klima
  $ python -m study_scraper ingest --source gesis    --topic klima

  studies (kept/pending/rejected): 15 / 0 / 0
  candidates seen / kept         : 23 / 21 (91.3%)
  studies per source (catalog)   : openalex 9, ssoar 6
  lake (source_records, kept)    : 5
    gesis 4, dawum 1
  lake per format                : gesis_kg_sparql_json 4,
                                   dawum_survey_json    1

## Older paradigm refocus (2026-05-28, A13)

Maintainer: **"We should not focus on pdf study extraction first.

## Paradigm refocus (2026-05-28, A13)

Maintainer: **"We should not focus on pdf study extraction first.
Future task. We want to focus on database data / structured data like
files and db."**

Implications, now written into the roadmap:

- **Phase 5c sources expansion reorders** to put structured-data
  sources first: DAWUM (JSON polling), Destatis GENESIS (statistical
  tables), Eurostat, GESIS DBK, BAMF data files, UBA Klimabilanz.
- **Polling-press-release source dropped** — those publish HTML / PDF;
  not structured. The same questions are answerable from DAWUM where
  available.
- **Think-tank SitemapSource dropped to tier 3** (deferred). Without
  PDF extraction, wiring discovery there just inflates catalog counts.
- **Phase 6-full PDF extraction stays on the roadmap but marked
  "future".**
- **New schema work:** a `datasets` notion sits alongside `studies`.
  See `DECISIONS.md` Q16.

## Source research (2026-05-28)

A full inventory of structured-data sources for German political
topics — ranked by direct yield for the example questions ("how many
Germans want X") — lives in
[`notes/structured-data-sources-2026-05-28.md`](notes/structured-data-sources-2026-05-28.md).

Headline finding: **GESIS** is the gold mine for issue-poll microdata
(Politbarometer 1977-2023, UBA Umweltbewusstsein 2018/20/22, ALLBUS,
GLES, ESS, EVS — all with structured downloads). **DAWUM** is the
gold mine for party polling. Statistical-office APIs (Destatis,
Eurostat) deliver indicators, not opinions.

Two new open questions raised by the research:
- **Q17** — GESIS auth handling (env vars; opt-in).
- **Q18** — microdata: file-only or row-level parsing? **Now moot
  under A14** (everything is payload-stored; row access is a view).

## /goal hit (2026-05-29): real data from 3 sources, both test-case questions answered

`docs/study_scraper/notes/real-data-3-sources-2026-05-29.md`.

Three sources ingest end-to-end against real, current 2025-2026 data
(harvested live via WebSearch since outbound HTTP from the dev
sandbox is blocked): SSOAR (6 academic studies), OpenAlex (10
records including 3 freshly harvested 2025 reports: Deutschland-
Monitor 2025 FGZ, Ipsos Earth Day 2025, Eurobarometer 538 2025), and
DAWUM lake (1 YouGov May 2026 Bundestag poll, n=1783, fanned to 8
typed party-result rows via `dawum_poll_results`).

  $ python -m study_scraper search klimaschutzgesetz
    80.0%  Eurobarometer 538 2025          —  EU climate priority
    62.0%  Forsa / Klima-Allianz (n=1009)  —  binding climate-law support
    44.0%  Sommer/Mattauch/Pahle 2022      —  stricter climate-law support

  $ python -m study_scraper search atomkraft
    65.0%  Civey longitudinal              —  Kernkraft nutzen (since mid-2022)
    55.0%  Innofact/Verivox March 2025     —  Wiedereinstieg
    36.0%  Innofact/Verivox March 2025     —  oppose

  $ python -m study_scraper view dawum_poll_results
    AfD 28 · Union 22 · SPD 13 · Grüne 13 · Linke 11 · FDP 4 · BSW 4 · Sonst 5
    (YouGov, fieldwork 2026-05-08 to 2026-05-11, n=1783)

151 tests pass.

## Lake live (2026-05-29; A14)

`source_records` table (migration 0005), `SourceRecord` model,
`study_scraper/sources/` namespace, lake ingest orchestrator
(`ingest.py`), DAWUM source, `ingest` and `view` CLI commands all
shipped. End-to-end smoke against real Postgres:

  $ python -m study_scraper ingest --source dawum --from-file <fix>
  lake run …: source=dawum seen=5 new=5 errors=0

  $ psql -c "SELECT party_shortcut, AVG(percentage)
             FROM   study_scraper.dawum_poll_results
             WHERE  parliament_name = 'Bundestag'
             GROUP  BY party_shortcut ORDER BY 2 DESC"
   party_shortcut | avg_pct
   ---------------+---------
    Union         |   30.25
    SPD           |   16.50
    Grüne         |   14.00
    AfD           |   12.38
    …

151 tests pass.

## Example-question measurement (2026-05-28)

Per maintainer: "Goal of the project is to answer political questions
based on the studies." Measured against two test questions:

- **"How many Germans want stricter climate laws?"** —
  `python -m study_scraper search klimaschutzgesetz` returns **62 %**
  (Forsa 2021, n=1009) and **44 %** (Sommer/Mattauch/Pahle 2022,
  n=6063) with source links and snippets.
- **"How many Germans want nuclear energy back?"** —
  `python -m study_scraper search atomkraft` returns **55 %** (Innofact
  März 2025, n=1003) plus 36 % oppose / 9 % undecided and **65 %**
  (Civey longitudinal seit Mitte 2022).

Mechanism works end-to-end. Coverage volume isn't there yet. See
`docs/study_scraper/notes/example-questions-2026-05-28.md` for the
full before / after and honest next-steps verdict.

## Paradigm note (2026-05-28)

Per A12, **coverage is now the primary metric**. The project is about
getting a broad overview of what exists across the universe of German-
political-topic studies. Precision, extraction quality, and other
dimensions are secondary and exist to make coverage useful for a human
reviewer. The dock manages precision; ingest favours recall. See
`GOAL.md` and `DECISIONS.md` A12.

## State at a glance

- **Knowledge base:** under `docs/study_scraper/`.
- **Decisions:** A1–A10 accepted. Q2, Q9, Q10 default-accepted (not
  blocking). Q11 (Supabase provisioning) tracked as non-blocking: the
  local-Postgres path is fully working end-to-end.
- **Code (Phase 2):** package skeleton, Typer CLI, topics CSV loader.
- **Code (Phase 3):** `Study` / `SurveyMetadata` / `Provenance` /
  `CrawlRun` models; SQL migration; Postgres storage adapter with
  upsert/list/migrate; `migrate` CLI; `study_scraper/docker-compose.yml`.
- **Code (Phase 4):** SSOAR OAI-PMH discovery source (live + fixture);
  rule-based topic filter ported from the legacy classifier, decoupled
  from ClickHouse; pipeline orchestrator (`run_one`) with idempotent
  upserts and crawl-run bookkeeping; `run` and `list` CLI commands.
- **Code (Phase 5, partial):** OpenAlex `/works` discovery source
  (live + fixture, abstract reconstruction from inverted index, DOI
  preferred canonical URL); per-upsert merge of `source_urls` and
  `topic_ids` arrays so re-discovery accumulates rather than
  overwrites; status / coverage report module used by both the CLI
  `status` command and the Streamlit dock; Streamlit control dock
  (`study_scraper/console/`) with Home (status) + Topics (editor with
  live "what would match" preview) per A11.
- **Milestone hit (2026-05-24):** "3 climate studies in DB" goal
  exceeded — 6 verified SSOAR studies. See
  `docs/study_scraper/notes/first-run-2026-05-24.md`.
- **Multi-topic broader run (2026-05-28):** 13 studies across 2 topics
  (klima, migration_einwanderung) × 2 sources (SSOAR, OpenAlex). See
  `docs/study_scraper/notes/multi-topic-run-2026-05-28.md`. Two real
  findings: migration topic vocabulary missing word forms
  (Migranten/Zuwanderer), cross-source dedup gap (same study, 2 URLs).
  Both queued in `TODO.md`.
- **Data shape:** documented in `docs/study_scraper/DATA.md` — schema,
  indexes, example row, common SQL queries.
- **Tests:** 86 pass — 10 OpenAlex parser tests, 3 status report tests,
  3 console tests (page compile + CSV writer), plus the Phase 4 suite.
- **Known sandbox limit:** the dev sandbox blocks outbound HTTP. Live
  modes of both sources share their parser with the fixture path that
  *is* tested; the maintainer verifies live by dropping `--from-file`.
- **Next:** Phase 5b — cross-source dedup (DOI fallback + title
  near-duplicate); Phase 5c — Bundestag DIP source; Phase 6 — stage-2
  semantic relevance.

This is an honest read of what's in the repo today, written as input to the
scraper pivot. It is **not** a release status report for the legacy ELT
platform.

## Overall

The repo is a relatively complex ELT setup (Airflow + ClickHouse + dbt +
Streamlit, ~9 Docker services) built around polling fixed API sources. The
maintainer's stated direction is to **deprioritize that machinery** and focus
on scraping the open web for relevant studies, driven by a topics sheet.

The repo's own `docs/PRD.md` is two lines and already pointed at this
direction:

> "Key is to brwose the connected data bases and the internet for as much
> representive issue polls a possible. reprensentative Umfragen in der
> Bevölkerung zu auf Policy Ebene"

So the pivot is more accurately described as **catching up to the original
PRD**, not changing direction. The build drifted into ELT plumbing.

## What works and is reusable

- **`config/topics/taxonomy.yml`** — well-structured topic taxonomy with id,
  description, multilingual synonyms, include/exclude keywords, dataset
  hints, examples. Schema is solid; should be the seed for the topics sheet.
  Currently has 4 topics: `steuern`, `klima`, `migration_einwanderung`,
  `bildung`.
- **`pipeline/topic_classifier.py`** — rule-based classifier reading the
  taxonomy. The matching logic (include / exclude / synonyms / dataset hints
  + scoring thresholds) is reusable for filtering scraped study candidates.
  ClickHouse loader coupling will need to be abstracted.
- **`connectors/base_connector.py`** — async `BaseConnector` pattern with
  rate limiter and aiohttp session lifecycle. Clean and reusable as a base
  for "study source fetchers".
- **Pydantic models, structlog, tenacity, async patterns** — solid stack
  primitives we can carry over.
- **Pre-commit, Ruff, Black, mypy, pytest config** — keep.

## What is broken and surprising

- **The Scrapy scaffold does not actually run.** `scraping/settings.py`
  references `scraping.pipelines.{ValidationPipeline,DuplicatesPipeline,
  StoragePipeline}`, `scraping.middlewares.{ErrorHandlingMiddleware,
  ProxyMiddleware,UserAgentMiddleware,RetryMiddleware}`, `scraping.items`,
  and `scraping.utils`. **None of those modules exist on disk.**
  `spiders/example_spider.py` imports `from ..items import ScrapedItem` and
  `from ..utils import clean_text, extract_date, validate_url` — also
  missing. Any `scrapy crawl` would fail at import time.
- This means the inherited summary's "Scrapy framework templated" overstates
  the situation: we have a `settings.py` and one example spider file with
  unresolved imports. **There is effectively no working scraper.**
- `docs/PRD.md` is two German/English lines with typos ("brwose",
  "reprensentative"). The detailed `architecture.md` describes the ELT
  platform, not the PRD's scraping goal.
- Project has been idle ~5 months (last commit Nov 2025: "minor changes").

## What is overbuilt for the new goal

- 9-container Docker stack with Airflow + Redis + Postgres + ClickHouse + dbt
  + Streamlit, for what is currently 5–8 DAGs and zero working scrapers.
- Star-schema dbt models in `dbt_project/models/` for analytics that mostly
  do not exist yet.
- Streamlit dashboard before there is interesting scraped data to show.

These aren't *wrong* — they're just **premature**. For a scraper iterating
toward a quality bar, a SQLite/DuckDB store + a Python CLI gets us further
faster.

## What's missing entirely for the new goal

- A working source-discovery layer (per-topic queries against open indexes
  and publisher search endpoints).
- HTML and PDF extraction pipelines.
- Deduplication across sources (same study often republished).
- Semantic relevance scoring (the rule-based classifier is necessary but not
  sufficient — many studies will use vocabulary that doesn't match the
  taxonomy keywords directly).
- An eval harness with a gold set, so iterations have a measurable target.
- A topics sheet format that a non-developer can edit (taxonomy.yml is
  developer-facing).

## Numbers (from inherited summary, not independently verified)

- 3 production connectors actively pulling data (DAWUM, GESIS, SOEP).
- ~7,171 lines of core code, 770+ transitive dependencies.
- ~86 files with unstaged local changes (on the maintainer's working copy,
  not in this branch).

## Implication for the pivot

Don't retrofit. Build the scraper as an **independent module** (`study_scraper/`)
that imports from the legacy code only where there's a clear win
(taxonomy loader, classifier rules). Treat ClickHouse, Airflow, dbt, and
Streamlit as **optional downstream sinks** — not as required infrastructure.
This is consistent with the maintainer's request that the new project be
"truly independent".
