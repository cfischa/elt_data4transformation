# ROADMAP — production-readiness backlog

This is the **developer agent's self-propose queue**. When there are no
open `agent:task` issues, the developer picks the top *buildable-now* item
here and ships ONE tested PR (see `.claude/skills/develop-feature`). Keep
items **small, concrete, testable, and in-scope** (only `study_scraper/**`,
`tests/**`, `docs/study_scraper/**`). The product agent re-prioritizes this
weekly from `GOAL.md` + live metrics.

Status keys: **[now]** buildable in CI today · **[needs-human]** creds/
installs/curation · **[done]** shipped.

## P1 — do these first (high value, clearly scoped, [now])

1. **HTTP resilience** (issue #32) — shared fetch helper for all source
   clients + fulltext: `tenacity` retries with exponential backoff and
   jitter, honour 429/503 `Retry-After`, and a small per-domain politeness
   delay in the fulltext loop (we currently burst dozens of PDFs at
   ssoar.info with no delay and no retry anywhere). Unit-test the policy
   with a mock transport.
2. **CI artifact-path bug** (issue #33) — `fulltext` saves PDFs to the
   Actions runner's local disk and writes that path into
   `studies.raw_artifact_ref`; the runner dies after the job, so the path
   dangles. Text + claims are safely in Postgres — the dangling ref only
   breaks refetch/reading-list assumptions. Make artifact storage
   CI-aware (skip local persist + leave ref NULL, or store bytes in
   Supabase storage) and re-queue accordingly.
3. **Eurostat typed SQL view(s)** — a migration projecting the JSON-stat
   `env_air_gge` (and `nrg_bal_s`) lake payloads into typed rows
   (geo, year, value), like `dawum_poll_results`, so the numbers are
   queryable. Migration + a view smoke check.
4. **Incremental harvesting** (issue #34) — SSOAR OAI supports
   `from=`/`until=`; store the last successful harvest timestamp per
   (source, topic) (derivable from `crawl_runs`) and pass it, instead of
   re-walking the same 400 newest records every run. For fulltext
   refetch, store `ETag`/`Last-Modified` and send conditional GETs
   (304 → skip). Standard incremental-crawl practice.

## Source-coverage plan — toward a representative platform

Current coverage: **catalog** SSOAR + OpenAlex (academic); **lake** DAWUM
(vote intention), GESIS KG (survey catalog), Eurostat (official stats).
The representativeness gaps are *issue-opinion* data and *official
statistics breadth*. Ranked by yield per effort:

5. **Eurobarometer** (issue #35) **[now]** — the EU Commission's
   standing opinion survey; free, structured, includes Germany, directly
   answers "what do people think about X". Lake source; data via the EC
   open-data portal / GESIS mirrors.
6. **Bundestag DIP API** **[needs-human: free API key by email]** —
   parliamentary documents (Drucksachen/Plenarprotokolle) frequently cite
   polling; structured JSON REST. Catalog-style source.
7. **BASE** **[now]** — OAI-PMH academic aggregator (Bielefeld);
   reuses the SSOAR OAI parser almost verbatim; widens the catalog far
   beyond SSOAR. Fixture + unit tests, no live call in CI.
8. **Polling-institute press releases** **[now, larger]** — Forsa, INSA,
   infratest dimap (ARD-DeutschlandTrend), Allensbach, YouGov DE, Civey
   publish issue-polls as HTML/PDF press pages. One config-driven
   `SitemapSource` (per-publisher YAML: sitemap/listing URL + selectors),
   feeding the existing PDF/fulltext + attribution machinery (A20/A21
   unblocked this tier). This is the single biggest issue-opinion win.
9. **Destatis GENESIS** **[needs-human: registration]** — official
   statistics REST API; scaffold + `from_file` tests buildable now.
10. **UBA / BAMF structured downloads** **[needs-human: sample files]** —
    XLSX/CSV lake sources.
11. **GESIS microdata (ALLBUS, Politbarometer)** **[needs-human:
    account/licences]** — the deepest issue-opinion source; revisit after
    the free tiers are exhausted.
12. *(skip)* wahlrecht.de — vote-intention aggregation duplicates DAWUM.

## Production craft — patterns from mature scrapers (investigated 2026-07-04)

Adopted checklist from incremental-crawl / scraping best practice
(conditional GET, fingerprinting, politeness — see AGENTS/ACCURACY docs
for our verification layers):

- **Fetch only what changed**: conditional GET (ETag/Last-Modified → 304)
  and content-hash fingerprints; we already hash payloads for idempotent
  upserts, but we re-download everything — item 4 closes this.
- **Politeness & backoff**: jittered delays, exponential backoff on
  429/503, robots.txt as a floor — item 1 closes this.
- **Two-layer bookkeeping**: "already scheduled" keys vs response metadata
  (etag, last fetch, checksum) — falls out of items 1+4.
- **Measure waste**: track duplicate rate / bytes fetched vs kept per run
  (extend `status --json`) so crawl spend is visible. **[now, small]**
- **Config-driven crawling**: topics now flow from `topics.csv` into the
  scheduled crawl automatically (done 2026-07-04); move the Eurostat code
  list into config too. **[now, small]**

## P3 — other [needs-human]

13. **Gold set + eval wiring** — replace the sample gold in
    `study_scraper/eval/gold/` with ~50 curated studies + ~40 tagged
    titles, then the harness reports a real accuracy number.
14. **spaCy lemmatization** (full German morphology) and **OCR** for
    scanned PDFs — both need host installs.

## Done (recent)
- PDF resolver, dock Attributions + Sources pages, auto-reviewer,
  cross-finding dedup, accuracy F1/F3/F4/F5/F6, eval harness scaffold.
- Agent tasks #20 (audit CLI), #21 (dock trust signals), #25 (openalex
  403 fulltext fallback), #26 root cause (OpenAlex OR-joins) — PRs #27–#30.
- Self-healing: no-op detection, RED runs, auto-filed/auto-closed ops
  ticket, ANTHROPIC_API_KEY alternative credential (PRs #23, #31).
- Topics: `atomkraft`, `wohnen`, `rente`, `verteidigung`; crawl topic list
  now derived from topics.csv.
