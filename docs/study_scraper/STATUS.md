# Status — independent analysis of the repo

_Last updated: 2026-07-05 (product-expansion build: answer-layer
B/C/D/E complete, monitoring digest, dossier + gap reports, Bundestag
DIP + opinion–policy gap, open dataset export. **392 tests pass**,
full loop verified end-to-end against a real Postgres.)_

## Product-expansion pass (2026-07-05)

Everything buildable from the strategy note
(`notes/product-expansion-2026-07-04.md`) shipped in one pass:

1. **Answer-layer correctness finished** — ROADMAP B (recency:
   `ask --since`, newer-poll dedup ties), C (sample size joined to
   findings: "[2025, n=1009]"), plus a real bug found on the way: %
   and n= claims from the same sentence collided on claim id, so
   sample sizes never reached the DB.
2. **Clustering (E) + aggregation (D)** — `clustering.py` (bilingual
   concept cosine, pluggable embedder) and `aggregate.py`; new CLI
   `answer` prints the weighted poll-of-polls view with spread.
3. **Monitoring v1** — migration 0009 (`watches`, `watch_snapshots`),
   `watch add/list/rm`, `digest` (≥5pt shift + novelty detection,
   Markdown artifact in the scheduled workflow).
4. **Dossier / gaps / policy-gap** — citable per-question Markdown
   dossier; per-topic evidence-gap table; opinion vs Bundestag-DIP
   juxtaposition (`policy-gap --topic X`).
5. **Bundestag DIP source** — catalog-style, fixture-tested, in the
   scheduled crawl (public API key default, `DIP_API_KEY` override).
6. **Open dataset export** — findings.csv + studies.csv + manifest.

Verified end-to-end on a local Postgres: 6-source fixture ingest →
claims → offline attribution (24 triples over 9 studies) → ask/answer
(German queries hit English questions via the semantic fallback) →
digest snapshot cycle (baseline → shift detection → settles at 0 news)
→ dossier/gaps/policy-gap/export. Three real defects found by the
production run and fixed with regression tests (claim-id collision,
ILIKE language miss, cluster over-merge at concept weight 3).

**Machine-bound remainder:** live crawl + fulltext need outbound
network (the dev environment's proxy policy blocks non-registry
hosts); scheduled production needs the `SCRAPER_POSTGRES_URL` secret
(RUNBOOK §1.1), optionally `DIP_API_KEY` and `ANTHROPIC_API_KEY`.

## Attribution pass (2026-06-15, A21)

Maintainer chose **Option A** for the LLM attribution layer. Shipped:
`study_scraper/extractors/llm_v1.py` (Anthropic API, prompt caching,
structured JSON, lazy import), `study_scraper/attribute.py`
(orchestrator), migration 0008 (`attributions` + `attribution_queue`),
storage helpers, and CLI `attribute` / `attribute-prompts` /
`attribute-apply` / `ask`.

Because Option A meters the operator's API key, the same module also
ships the **offline Cowork path** (`attribute-prompts` → answer in a
Cowork session → `attribute-apply`) — identical rows, zero extra API
spend. Model defaults to `claude-opus-4-8`; `STUDY_SCRAPER_LLM_MODEL`
overrides for cheap bulk. End-to-end offline smoke verified:
`ask "climate"` → `62.0% support  Stricter climate laws`.

**Maintainer's next move: run [`RUNBOOK.md`](RUNBOOK.md) §0 on a
networked machine** (DB up, first live crawl, fulltext, status), then
report the `status` numbers so the next iteration works from real data.

## Production pass (2026-06-11, A20)

Maintainer: bring the product to production; the goal is full-document
statistics ("not just abstracts"), packaged so only machine-bound
steps remain.

Shipped:

1. **Full-document extraction pipeline** (`study_scraper/fulltext.py`,
   A20) — fetch each kept study's `canonical_url` (PDF or HTML),
   extract the full text (pypdf / BeautifulSoup), run the claim
   extractor over the whole body (`extractor='regex-v2'`), store the
   raw artifact on disk with `studies.raw_artifact_ref`. Abstract
   claims (regex-v1) and full-text claims (regex-v2) coexist; each
   extractor re-runs idempotently. CLI: `study_scraper fulltext
   [--limit N] [--study-id ID] [--refetch]`.
2. **Reading list** (migration 0007) — the hybrid model's second
   track: kept studies with zero claims, with reason `no_artifact`
   (fetch pending) or `no_claims` (human must read). CLI:
   `study_scraper reading-list`.
3. **Reference follower step 2** (`study_scraper/follow.py`) —
   `study_scraper follow` lists OpenAlex works cited by our studies
   but not yet ingested; `--fetch --topic <id>` ingests them through
   the normal pipeline (batched ID-filter queries; dedup makes
   over-fetching safe). OpenAlexSource gained `work_ids` mode.
4. **Scheduled crawl** — `.github/workflows/scrape.yml`: Mon+Thu
   crawl of all 5 sources + follower + fulltext + status artifact.
   Activates when the `SCRAPER_POSTGRES_URL` repo secret exists;
   exits green otherwise.
5. **`status --json`** for cron/CI consumers.
6. **[`RUNBOOK.md`](RUNBOOK.md)** — the machine-only checklist: DB
   provisioning, first live crawl (with known-unknowns to watch),
   scheduling, operator routine, honest not-done list.

Verified end-to-end in sandbox: ingest → fulltext over a real PDF
(5 claims extracted from document body) → claims searchable → reading
list shows the right reasons → status --json well-formed.

## Second clean+build pass (2026-05-31, late)

## Second clean+build pass (2026-05-31, late)

Three things shipped this pass:

1. **Eurostat lake source** (A19) — `study_scraper/sources/eurostat.py`.
   Public dissemination API; JSON-stat 2.0 payloads land in
   `source_records` as-is. CLI: `ingest --source eurostat --code
   env_air_gge --code nrg_bal_s`. Fixture covers real codes
   (env_air_gge GHG emissions; nrg_bal_s energy balances).
2. **OpenAlex citation graph capture** (A18, Phase 5d step 1) —
   `referenced_works[]` / `related_works[]` IDs from each Work end up
   in `Candidate.raw` and in `Study.provenance`. Queryable via
   `provenance->'referenced_works'`. Foundation for the active
   reference-follower (Phase 5d step 2, future).
3. **Dock Lake browser** — `study_scraper/console/pages/3_Lake.py`.
   Filter by source / format / topic / status; expand a row to inspect
   its raw JSONB payload; metric tiles for the current filter.

End-to-end across **five sources** now runs:

  $ python -m study_scraper run    --source ssoar    --topic klima
  $ python -m study_scraper run    --source openalex --topic klima
  $ python -m study_scraper ingest --source dawum    --topic klima
  $ python -m study_scraper ingest --source gesis    --topic klima
  $ python -m study_scraper ingest --source eurostat --topic klima \
        --code env_air_gge --code nrg_bal_s

  studies (kept/pending/rejected): 15 / 0 / 0
  candidates seen / kept         : 25 / 23 (92.0%)
  studies per source (catalog)   : openalex 9, ssoar 6
  lake (source_records, kept)    : 7
    gesis 4, eurostat 2, dawum 1
  lake per format                : gesis_kg_sparql_json 4,
                                   eurostat_jsonstat    2,
                                   dawum_survey_json    1

## First clean + build pass (2026-05-31)

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
