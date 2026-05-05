# Status — independent analysis of the repo

_Last updated: 2026-05-05 (decisions Q1, Q3–Q8 resolved; Phase 2 starting)._

## State at a glance

- **Knowledge base:** bootstrapped under `docs/study_scraper/`.
- **Decisions:** A1–A10 accepted. Q2, Q9, Q10 default-accepted (not
  blocking). New non-blocking Q11 tracks Supabase provisioning.
- **Code:** none yet under `study_scraper/`. Phase 2 (project skeleton) is
  unblocked and starting in this iteration.
- **Stack chosen:** Supabase (Postgres + Storage) for storage, Typer CLI
  for orchestration, two-stage relevance (rules + local embeddings),
  topics edited as CSV.

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
