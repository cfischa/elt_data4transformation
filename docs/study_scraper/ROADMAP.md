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

Re-prioritized 2026-07-20: collection just took its biggest jump yet
(2,593 → 4,880 studies since 07-13) while the two consumption-side
pipes stayed flat or got worse. See issue #8 for the live metrics
behind this reorder.

1. **Attribution throughput** (issue #49) **[needs-human]** — `claims`
   is at 5,412 rows (was 4,369 on 07-13, +992 in the 2026-07-20 crawl
   alone) against only **56 attributions ever written**. The gap is
   now ~4,700 unattributed claims and widening every crawl.
   `scheduled-attribute` runs twice-weekly at `--limit 40` — the
   developer agent confirmed (2026-07-19) the fix requires editing
   `.github/workflows/attribute.yml`, which is out of both agents'
   edit scope. **Needs the maintainer** to raise the cadence/limit
   directly, or authorize an agent to touch `.github/**` for this one
   change. This is still the single biggest lever: 4,880 collected
   studies are only as useful as the structured `(question, position,
   %)` triples we actually extract from them.
2. **Scheduled-scrape reliability** (issue #53) **[now, priority:high,
   new 2026-07-20]** — 3 consecutive `scheduled-scrape` runs
   (07-13, 07-16, 07-20) have stalled for hours on an unbounded
   `Retry-After` wait in `study_scraper/http.py::_wait_seconds` when
   OpenAlex 429s, burning most/all of the 5h job budget. Cap the
   honoured `Retry-After` the same way the exponential-backoff branch
   already is. Pure code fix, in scope, no dependencies — promoted
   ahead of item 3 because a stalling crawl risks silently reducing
   collection frequency, which undermines the coverage-first goal.
3. **`bundestag_dip` still fully broken** (issue #48, reopened
   2026-07-20) **[needs-human]** — the silent-401 visibility fix
   landed (now shows `errors=1` instead of a clean `seen=0` row), but
   the source has **0 studies ever ingested**. It's our only
   pure-government source, blocking the GOAL.md source-coverage
   category requirement. Needs a fresh `DIP_API_KEY` (free, mail to
   infoline.id3@bundestag.de) set as a repo secret.
4. **Topic-content gap** (issue #50, bumped to **priority:high**
   2026-07-20 — 3+ weeks overdue) **[now]** — maintainer's 2026-06-26
   ask (Erbschaftssteuer keywords on `steuern`, new `russland_ukraine`
   topic) never landed. Pure `topics.csv` + `questions.yml` change, no
   code risk, no blockers — just hasn't been picked up.
5. **Eurostat typed SQL view(s)** — a migration projecting the JSON-stat
   `env_air_gge` (and `nrg_bal_s`) lake payloads into typed rows
   (geo, year, value), like `dawum_poll_results`, so the numbers are
   queryable. Still only 3 lake rows total (2 codes × geo=DE) so this
   stays lower-value until the code list grows — keep it after 1–4.

## Answer-layer statistics — correctness upgrades (audited 2026-07-04; B+C = issue #39)

A. **Population in dedup identity** **[done 2026-07-04]** — same
   question+% among different populations no longer merge.
B. **Recency-aware answers** **[done 2026-07-05]** — `ask --since YEAR`;
   dedup representative prefers newer publication on confidence ties.
C. **Sample-size context** **[done 2026-07-05]** — the study's
   representative n= claim joins its attributions; `ask` shows
   "[2025, n=1009]". (Also fixed: % and n= claims from the same
   sentence used to collide on claim id and the n was dropped.)
D. **Poll-of-polls aggregation** **[done 2026-07-05]** — `answer <q>`:
   recency- (3y half-life) and sample-size- (sqrt(n/1000), clamped)
   weighted mean per (question-cluster, position) with spread, poll
   count, year range, Σn. Never a lone number.
E. **Semantic question clustering** **[done 2026-07-05, v1 offline]** —
   `clustering.py`: bilingual concept-map token cosine, greedy
   single-linkage; 'Atomausstieg rückgängig machen' and 'return to
   nuclear power' cluster. `embedder=` hook ready for a real embedding
   backend (pgvector) later. German queries reach English-normalized
   questions via the same map (`search_attributions_semantic`).

## Product-expansion build (2026-07-05; see notes/product-expansion-2026-07-04.md)

- **Monitoring v1** **[done]** — migration 0009 `watches` +
  `watch_snapshots`; `watch add/list/rm`; `digest` reports ≥5pt shifts
  and newly tracked questions vs the previous snapshot; scheduled
  workflow uploads `opinion-digest.md` per run.
- **Research dossier** **[done]** — `dossier <q> [--out]`: citable
  Markdown (summary, per-finding table, method caveats, provenance).
- **Evidence-gap report** **[done]** — `gaps [--topic]`: per question
  cluster the freshness/breadth flags (stale, single source, no
  percentages).
- **Opinion–policy gap** **[done, v1 juxtaposition]** — `policy-gap
  --topic X`: aggregated opinion vs ingested Bundestag DIP Drucksachen.
  Per-question → bill matching is the v2 once DIP coverage grows.
- **Open dataset export** **[done]** — `export --out DIR`: findings.csv
  + studies.csv + manifest; facts and links only.
- **Next**: real embedding backend for E; demographic breakdowns
  (population_segment); opinion⇄fact joins (needs Eurostat typed
  views, P1.3); public read-only surface (gated on gold-set eval).

## Source-coverage plan — toward a representative platform

Current coverage (2026-07-20 live DB): **catalog** OpenAlex (4,548) +
SSOAR (332) — academic, openalex dominates volume; **lake** DAWUM
(vote intention, 3,868 rows), GESIS KG (survey catalog, 500), Eurostat
(official stats, 3 — thin by design), **Eurobarometer (0 — built,
never run, see #65)**. **Bundestag DIP is wired but has 0 studies ever**
— see #48 (reopened). The representativeness gaps are *government
coverage* (the one category with zero working sources) and *official
statistics breadth*. Ranked by yield per effort:

5. **Eurobarometer** (issue #35) **[done, code — but see #65]** — shipped
   2026-07-15 as A24 (`study_scraper/sources/eurobarometer.py`, GESIS KG
   SPARQL filtered to Eurobarometer waves). **Never actually run**: not
   in `.github/workflows/scrape.yml`'s hardcoded source list, so it has
   0 live records. #65 tracks the one-line, maintainer-actioned wiring.
6. **Bundestag DIP API** **[broken since 2026-07-06, reopened #48]** —
   `discovery/bundestag_dip.py`, catalog-style, fixture-tested; in the
   scheduled crawl, but the hardcoded public API key now 401s on every
   request (0 records ingested, now visible as `errors>0` — the
   silent-failure half of #48 is fixed). Needs a fresh personal key
   (free by mail to infoline.id3@bundestag.de).
7. **GovData.de (CKAN)** (issue #64, **new, scouted 2026-07-20**)
   **[now]** — Germany's cross-government open-data catalog (federal +
   state + municipal metadata in one CKAN instance); free, no-auth
   `package_search` REST API, "Data License Germany 2.0" with
   per-dataset overrides captured explicitly. Broader fix for the
   government-category gap than #48 alone — one integration can surface
   BMAS/UBA/BAMF/Destatis datasets we'd otherwise need N separate
   scrapers for.
8. **BASE** **[now]** — OAI-PMH academic aggregator (Bielefeld);
   reuses the SSOAR OAI parser almost verbatim; widens the catalog far
   beyond SSOAR. Fixture + unit tests, no live call in CI.
9. **Domain-audit source discovery** (issue #38) **[done, CLI]** — Phase
   5d: `study_scraper sources-audit [--limit]` walks stored study/
   reference URLs, groups by registrable domain, and surfaces domains
   with no dedicated source, ranked by frequency (see DECISIONS.md A30).
   Dock/Streamlit surface still open. Plus: the product agent now
   scouts via WebSearch each run.
10. **Polling-institute press releases** **[now, larger]** — Forsa, INSA,
    infratest dimap (ARD-DeutschlandTrend), Allensbach, YouGov DE, Civey
    publish issue-polls as HTML/PDF press pages. One config-driven
    `SitemapSource` (per-publisher YAML: sitemap/listing URL + selectors),
    feeding the existing PDF/fulltext + attribution machinery (A20/A21
    unblocked this tier). This is the single biggest issue-opinion win —
    but see #49 first: more raw studies don't help while attribution is
    the bottleneck, not collection.
11. **Destatis GENESIS** **[needs-human: registration]** — official
    statistics REST API; scaffold + `from_file` tests buildable now.
12. **UBA / BAMF structured downloads** **[needs-human: sample files]** —
    XLSX/CSV lake sources. Possibly superseded by #64 (GovData.de) if
    it surfaces the same datasets via one integration — check before
    building bespoke scrapers.
13. **GESIS microdata (ALLBUS, Politbarometer)** **[needs-human:
    account/licences]** — the deepest issue-opinion source; revisit after
    the free tiers are exhausted.
14. *(skip)* wahlrecht.de — vote-intention aggregation duplicates DAWUM.
15. **bpb.de "Ukraine-/Länder-Analysen"** (Forschungsstelle Osteuropa
    Bremen, scouted 2026-07-13) — free, no auth, periodic HTML analysis
    series that regularly cites representative war-opinion surveys
    (German and Ukrainian). Tier-3 (HTML, no API) — candidate for the
    `SitemapSource` config (item 10) once that tier is built, not
    buildable standalone.
16. **eupinions** (Bertelsmann Stiftung quarterly EU opinion survey,
    scouted 2026-07-13) — same tier-3 bucket as #15; blog/PDF
    publication, no API found. Lower priority than #15 — Bertelsmann is
    already on the think-tank list and the data feed feels less durable.

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
- HTTP resilience (#32), CI artifact-path fix / A23 `persist_artifacts`
  (#33), incremental harvesting / OAI `from=` windows + conditional GET
  (#34, PR #47) — all closed as of 2026-07-13.
- Eurobarometer source built (A24, #35) — landed 2026-07-15 but not yet
  wired into the scheduled crawl, see #65.
- `bundestag_dip` silent-401 visibility fix (#48 half 2 of 2) — now
  correctly shows `errors>0`; the source itself is still broken (key).
- Attribution queue reorders registry-topic studies first (A26, #59
  item 1) — landed 2026-07-19; items 2 (console Questions page) and 3
  (lake→answers mapping proposal) still open.
