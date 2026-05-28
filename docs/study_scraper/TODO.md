# TODO — ordered backlog

Work flows top-to-bottom. **Do not skip ahead** without updating this file
and `STATUS.md`. Items marked **[BLOCKED]** require a `DECISIONS.md` answer
before they start.

## Phase 0 — Knowledge base bootstrap

- [x] Create `docs/study_scraper/` and write `README.md`, `GOAL.md`,
      `STATUS.md`, `DECISIONS.md`, `TODO.md`.
- [x] Document the broken Scrapy scaffold finding.
- [x] Surface design decisions Q1–Q10 to the maintainer.

## Phase 1 — Decisions locked in

- [x] Maintainer answered Q1, Q3–Q8. Open questions Q2, Q9, Q10 default-
      accept; new follow-up Q11 (Supabase provisioning) is non-blocking
      for offline dev.
- [x] Decisions A4–A10 added to `DECISIONS.md`.

## Phase 2 — Project skeleton (in progress)

- [ ] Create `study_scraper/` package: `__init__.py`, `cli.py`, `config.py`,
      `models.py`, `topics.py`, `storage/`, `discovery/`, `extraction/`,
      `pipeline.py`. Include a `study_scraper/README.md` that points back
      to this knowledge base.
- [ ] Add `study_scraper` to `pyproject.toml` packages and pin the minimal
      dependency set (httpx, pydantic, pydantic-settings, pyyaml, typer,
      beautifulsoup4, pypdf, structlog, tenacity, asyncpg, supabase-py).
      Embeddings dependency (sentence-transformers) added when Phase 6
      lands.
- [ ] Seed `config/topics/topics.csv` from the 4 topics in `taxonomy.yml`.
- [ ] Add a `Makefile` target: `make scrape TOPIC=<id>` and `make
      scrape-eval`.
- [ ] Smoke test: `python -m study_scraper --help` and a unit test that
      loads the CSV.

## Phase 3 — Data model + storage layer (done — except dedup tail)

- [x] `Topic` Pydantic model + CSV loader. (Phase 2)
- [x] `Study` Pydantic model with sha256-of-canonical-URL id, validators
      for timezone-aware fetched_at and unit-interval topic_scores.
      `SurveyMetadata`, `Provenance`, `CrawlRun` models added.
- [x] SQL migrations under `study_scraper/migrations/` (Postgres-
      compatible). Schema: `studies`, `crawl_runs`, `crawl_run_studies`,
      `schema_versions`. GIN index on `topic_ids`.
- [x] Synchronous Postgres storage adapter (`study_scraper/storage/`).
      Reads `POSTGRES_URL` first; derives a Postgres URL from
      `SUPABASE_URL` + `SUPABASE_SERVICE_KEY` for hosted Supabase.
      SQLite is explicitly unsupported (A7).
- [x] `python -m study_scraper migrate` command; idempotent.
- [x] Local-dev docker-compose at `study_scraper/docker-compose.yml`
      pinning `supabase/postgres:15.1.0.117` on port 5544 (no conflict
      with the legacy Airflow Postgres on 5432).
- [x] 12 storage integration tests against real Postgres
      (run via `STUDY_SCRAPER_TEST_DSN`); skipped if env var absent.
- [ ] Deduplication beyond canonical-URL hash (DOI fallback + title
      near-duplicate via rapidfuzz). Deferred into Phase 4 — easier to
      decide thresholds once we see real SSOAR candidates.

## Phase 4 — One source end-to-end (DONE)

- [x] `DiscoverySource` Protocol + `Candidate` model in
      `study_scraper/discovery/base.py`.
- [x] First source: **GESIS SSOAR** via OAI-PMH (`oai_dc` metadata
      prefix). Synchronous iterator. Supports both live (HTTP to
      ssoar.info) and `--from-file` (local OAI XML fixture) modes that
      share the same parser.
- [x] Stage-1 rule-based topic filter ported from
      `pipeline/topic_classifier.py` into
      `study_scraper/topic_filter.py`, decoupled from ClickHouse.
      Default `min_score=0.2` for SSOAR (one include keyword anywhere).
      Per-topic exclude keywords short-circuit.
- [x] Pipeline orchestrator (`study_scraper/pipeline.py::run_one`): for
      each candidate, applies filter, promotes to `Study`, upserts,
      records `CrawlRun` and `crawl_run_studies` junction.
- [x] `has_quantitative_data` heuristic (cheap, noisy by design —
      gates nothing, just hints).
- [x] CLI commands: `run --source ssoar --topic <id> [--from-file PATH]
      [--limit N] [--min-score F]`, `list --topic <id>`.
- [x] First-run notes at `docs/study_scraper/notes/first-run-2026-05-24.md`.
- [x] **Milestone:** 6 verified climate-relevant SSOAR studies
      persisted to Postgres (maintainer goal "3 studies in DB"
      exceeded). Idempotent re-runs confirmed.
- [ ] HTTP fetcher polish: tenacity retries, robots.txt check, custom
      backoff on 429. Deferred — current SSOAR client is a thin
      `httpx.Client` without retries. Bring in when adding OpenAlex
      (which has stricter rate limits).
- [ ] PDF + HTML artifact download. Deferred — Phase 4 captures
      metadata + abstract from OAI Dublin Core, which is enough for
      topical relevance. Full-text fetching lands when stage-2
      semantic scoring needs it (Phase 6).

## Phase 5 — Second + third source (in progress)

- [x] **OpenAlex source** (`study_scraper/discovery/openalex.py`) —
      `/works` JSON, abstract reconstruction from inverted index,
      DOI-preferred canonical URL, `from_file` fixture mode. 10 unit
      tests against a real-DOI fixture.
- [x] **Multi-topic, multi-source ingest** validated end-to-end
      against real Postgres: 13 studies across 2 topics × 2 sources;
      see `docs/study_scraper/notes/multi-topic-run-2026-05-28.md`.
- [x] **Status / coverage report** (`study_scraper/status.py`) — used
      by the CLI `status` command and the Streamlit dock; 3 integration
      tests.
- [x] **Streamlit control dock** (`study_scraper/console/`) — Home page
      (status overview) + Topics page (editor with live "what would
      match" preview against current DB). Scope-limited by A11. 3
      tests (page compile + CSV writer round-trip).
- [ ] **Cross-source dedup** (Phase 5b) — DOI fallback +
      title-near-duplicate (rapidfuzz). Made concrete by the multi-
      topic run: the same Erdgas study appeared as 2 rows under SSOAR
      and OpenAlex. Pre-req for fair source-coverage counts.

## Phase 5c — Sources expansion (primary metric is coverage; A12)

A12 reframed: more sources = more coverage. The earlier "pull DAWUM
only if a gap shows up" is overturned. Plan: wire **every source on
this list** behind the `DiscoverySource` interface. Order is
effort-ascending; each entry has a one-line live-mode plan + a
fixture for sandbox dev.

Academic / repository (lowest-friction, highest yield):
- [ ] **DAWUM** — port the legacy `connectors/dawum_connector.py`
      behind `DiscoverySource`. Polling-aggregator-shaped data;
      especially valuable for topical surveys (klima, migration,
      steuern).
- [ ] **BASE** (Bielefeld Academic Search Engine) — OAI-PMH
      compatible; same parser shape as SSOAR.
- [ ] **CORE** — REST API, free key, broad academic coverage.
- [ ] **GESIS DBK** (Datenbestandskatalog, the wider GESIS catalog
      beyond SSOAR) — likely SPARQL via the existing legacy GESIS
      connector; port behind interface.

Government:
- [ ] **Bundestag DIP** — REST API at
      `https://search.dip.bundestag.de/api/v1/`, free key by email.
      Drucksachen, Sachstandsberichte, Vorgänge.
- [ ] **Destatis GENESIS** — port the existing legacy connector.
- [ ] **Umweltbundesamt (UBA)** — has a structured publications list
      (HTML + RSS). Climate-heavy.
- [ ] **BAMF Forschungszentrum** — Migration / Integration. Has a
      publications page with a sitemap.

Think tanks (heterogeneous; one source class can cover several with a
config-driven scraper):
- [ ] **SWP** (Stiftung Wissenschaft und Politik) — RSS + sitemap.
- [ ] **DIW Berlin** — RSS + publications page.
- [ ] **Ifo** — RSS.
- [ ] **Bertelsmann Stiftung** — sitemap.
- [ ] **FES** (Friedrich-Ebert-Stiftung) — sitemap.
- [ ] **KAS** (Konrad-Adenauer-Stiftung) — sitemap.
- [ ] **Sachverständigenrat** — publications page.

Civic society / data:
- [ ] **wahlrecht.de** — long-running election + polling site.

Implementation note: rather than 12 bespoke classes, group think tanks
behind one **sitemap/RSS source** parameterised by per-publisher YAML
config. The interface stays the same; the operator adds new think
tanks by editing config, not Python.

## Phase 5d — New-source discovery (per A12)

"Browsing tool which finds new studies/sources which are not already
in the standard list" — the missing capability the maintainer flagged
2026-05-28. Coverage isn't bounded by the source list we hard-code;
the tool must keep finding new sources too.

Three mechanisms, ranked by yield / effort:

- [ ] **Reference / related-works follower** — when we ingest an
      OpenAlex Work, follow `referenced_works[]` and `related_works[]`
      one hop. Each becomes a candidate `Study`. Cheap recall booster.
      We already capture these IDs (need to expand the OpenAlex source
      to write them to `provenance`).
- [ ] **Domain audit** — periodic WebSearch (or, on the maintainer's
      machine, real web search) for `<topic include keyword> studie
      Deutschland`, capture result URLs, group by domain. Domains we
      don't have a source for surface as **candidate sources** in the
      dock for human review (accept → add to the sitemap-source YAML).
- [ ] **Citation-context mining (Phase 6+)** — when we have the full
      text of a study (PDF extracted), find URLs and DOIs in the
      references section, normalize them to candidate studies. Defer
      until PDF extraction lands.

Control-dock surface: a third page **"Sources"** with
  - the table of known sources (id, kind, last successful run, study
    count, error count),
  - a queue of **candidate sources** discovered by the audit, with
    accept / reject buttons,
  - a queue of **candidate studies** discovered by the
    reference-follower but not yet ingested.

## Phase 6 — Stage-2 semantic relevance (unchanged from earlier plan)

Lower priority now that A12 emphasises recall over precision; remains
in the roadmap so that the *human* reviewer's view of candidates is
ranked sensibly, even though we no longer use it as an ingest gate.

## Phase 7 — Eval harness (deferred, post-Phase 5c)

Per A10, gold set is built after we have real candidates to curate.
Once Phase 5c has the broader source set ingesting, the gold set's
coverage estimates become meaningful.

## OPEN — needs maintainer call (Discuss-if-not-sure questions)

These touch design, not just sequencing. Calling them out so the next
build pass doesn't bake in an assumption you'd reverse.

### Q12. Keep candidates below the threshold (recall over precision)?
**Proposal:** Add a `candidates` table parallel to `studies`. Anything
the source yields and survives the exclude-keywords short-circuit gets
written there, regardless of score. Promotion to `studies` happens
when score ≥ threshold OR a human accepts in the dock. This makes the
human-in-the-loop precision story honest — nothing is dropped silently
just for being borderline.
**Cost:** one new table, a "review queue" page in the dock, doubles
write volume.
**Need:** yes/no on the proposal. (My recommendation: yes, this is
the cleanest expression of A12.)

### Q13. Reset success-criteria thresholds in GOAL.md?
**Proposal:** I rewrote GOAL.md's success criteria for the new
framing (source count ≥ 8, studies-per-topic ≥ 50 on ≥ 3 topics,
recall ≥ 80 % on a gold set, plus secondary precision ≥ 70 %, fields
≥ 90 %). These are guesses — they need your ratification or override.
**Need:** confirm the numbers, or send replacements.

### Q14. Think tanks as one config-driven source or N source classes?
**Proposal:** one **SitemapSource** class taking a YAML config that
lists `(publisher_id, sitemap_url, css_selectors_for_metadata)`. The
operator adds a new think tank by editing YAML, not Python. Trades a
bit of indirection for a lot less code to maintain.
**Need:** yes/no. (My recommendation: yes.)

### Q15. New-source discovery — how aggressive?
**Proposal:** all three mechanisms (reference-follower, domain audit,
citation mining) are useful eventually. For now, build the **reference
/ related-works follower first** because we already have the data
(OpenAlex returns these IDs for free). Domain audit lands when the
dock's Sources page exists. Citation mining waits for PDF extraction.
**Need:** confirm the order, or override.

## Phase 6 — Stage-2 semantic relevance

- [ ] Add a sentence-embeddings dependency (sentence-transformers,
      multilingual model).
- [ ] Implement `study_scraper/topic_filter_semantic.py`. Compute
      similarity between topic description+keywords and study abstract /
      first-page text. Store as `topic_scores`.
- [ ] Combine stage 1 + stage 2 with a per-topic threshold.

## Phase 7 — Eval harness (was Phase 5; reordered per A10)

- [ ] After the first end-to-end run produces real candidates, work with
      the maintainer to curate gold sets at
      `docs/study_scraper/eval/gold/<topic_id>.yml` (≥20 entries per
      topic).
- [ ] Implement `python -m study_scraper eval --topic <id>`: runs the
      pipeline, compares to the gold set, writes a markdown report under
      `docs/study_scraper/eval/reports/`.
- [ ] Add the report to the PR template so every iteration ships with
      numbers.

## Phase 8 — Iterate to the goal

- [ ] Tighten thresholds, expand to tier-2 sources, push metrics toward
      the bar in `GOAL.md`.
- [ ] When the success criteria are met on ≥3 topics, write a wrap-up
      note in `STATUS.md` and open the discussion of step 2 (data
      engineering — separate project).

---

## Anti-goals (don't do these without an explicit decision)

- Do not introduce Airflow/dbt/Streamlit dependencies into
  `study_scraper/`. ClickHouse is allowed only as the documented escape
  hatch from A7, not as the default.
- Do not extend `scraping/` — see `DECISIONS.md` A3.
- Do not scrape behind logins or paywalls.
- Do not add more than 3 sources before stage-2 semantic filtering and
  the eval harness exist. Without measurement we can't tell if we're
  improving.
- Do not start a UI before the data is stable and useful.
