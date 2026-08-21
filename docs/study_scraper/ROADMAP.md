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

Updated 2026-08-17: `bundestag_dip` **regressed back to 401** (#106,
DECISIONS.md A40) after only 2 working scheduled runs (08-06, 08-10) —
the no-key mirror A35 switched to on 08-03 has started enforcing auth
again, converging back to #48's original blocker (needs a real personal
`DIP_API_KEY`; no code fix possible). Stuck at **1,115 studies** since
08-10 while `openalex`/`ssoar` keep growing. **`GOAL.md`'s topic-coverage
criterion stays MET** (all 8 topics ≥680 studies from ≥3 sources — a
source going quiet doesn't un-ingest what it already contributed). The
*source-coverage* criterion (≥8 production sources) is unchanged: still
only 6 distinct sources have ever produced a successful `crawl_runs` row;
core/eurobarometer/govdata remain built, fixture-tested, and simply never
added to `scrape.yml` (#65). **New this week:** the attribution pipeline
went fully dark for 6+ days (76 attributions since 08-11, zero net-new),
including a 2026-08-14 run that reported success (17 turns, $0.87 cost)
but wrote zero `attribution_attempts` rows — a distinct regression from
the known cadence bottleneck. Filed #110 (priority:high, in-scope) so
`status --json` surfaces this kind of silent stall automatically instead
of needing manual Postgres-timestamp tracking in issue comments.

1. **Wire core + eurobarometer + govdata into the scheduled crawl**
   (issue #65, priority:high, needs-human) — three ready-to-run
   sources, zero live records, one `scrape.yml` edit each (exact lines
   in the issue). Still the highest-leverage ask: it directly
   clears `GOAL.md`'s ≥8-production-sources bar in one PR.
2. **Attribution throughput + dark-pipeline stall** (issue #49)
   **[needs-human]** — `claims` at 9,202 rows and still climbing every
   crawl; `attributions` stuck at **76** since 2026-08-11 (6+ days,
   including a run that "succeeded" but wrote zero rows — see #49's
   08-14/08-17 comments). The cadence half
   (`scheduled-attribute`'s twice-weekly/`--limit 40` in
   `.github/workflows/attribute.yml`) is still out of both agents' edit
   scope; #110 (new, in-scope) adds a staleness signal so this doesn't
   require manual tracking again. Still the single biggest lever on the
   *answering* half of the goal.
3. **Topic-content gap** (issue #50, priority:high, open since
   2026-06-26 — 7+ weeks) **[needs-human]** — maintainer's ask
   (Erbschaftssteuer keywords on `steuern`, new `russland_ukraine`
   topic) needs `config/topics/topics.csv` + `questions.yml`, both
   outside `study_scraper/**`/`tests/**`/`docs/study_scraper/**`, so
   neither agent can build it as scoped. Scouted 2026-08-17 for an API
   alternative to hand-editing keywords (a structured Russia/Ukraine
   opinion-data source) — found none free/no-auth; stays a config edit.
4. **`bundestag_dip`** (issue #48, reopened in spirit as #106/A40) —
   **regressed 2026-08-13**, still 401ing 2026-08-17. PR #91's no-key
   mirror (A35/A36) worked for exactly 2 scheduled runs before the
   mirror itself started requiring auth. No further code fix available;
   converges back to needing a real `DIP_API_KEY`
   (`infoline.id3@bundestag.de`).
5. **Eurostat typed projection** (issue #86) **[done 2026-08-02]** —
   `study_scraper/jsonstat.py::flatten_jsonstat` decodes the JSON-stat
   `id`/`size`/`dimension`/`value` encoding into typed rows (dimension
   labels + value); `eurostat-table --code <code>` is the queryable
   surface. Python, not a SQL view — see DECISIONS.md A34 for why.
6. **OpenAlex 429s structurally starving `rente`/`verteidigung`**
   (issue #71) **[done 2026-07-30]** — topic-crawl order now rotates by
   `GITHUB_RUN_NUMBER` (A32). Confirmed working live 2026-08-03: both
   topics' openalex counts roughly doubled/tripled week over week.
7. ~~SSOAR outage, 8/8 topics failed 2026-08-10~~ **[done/closed, issue
   #100]** — confirmed transient upstream outage: `ssoar` ran clean on
   both the 2026-08-13 and 2026-08-17 scheduled runs (`errors=0` across
   all 8 topics each time; live `status` on 2026-08-18 shows 625 total
   SSOAR studies still growing). No repeat since 08-10 — no retry/backoff
   hardening needed per the issue's own acceptance criteria.

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

Current coverage (2026-08-17 live DB): **catalog** OpenAlex (5,321) +
SSOAR (625), and **Bundestag DIP (1,115, stalled since 08-10 — 401ing
again, #106/A40)** — academic + government, three sources have fed the
`studies` table (GOAL.md's topic-coverage bar is met and stays met, see
P1 intro); **lake** DAWUM (vote intention, ~3,900 rows), GESIS KG
(survey catalog, 500), Eurostat (official stats, 3 — thin by design).
**CORE, Eurobarometer, GovData.de are all built and fixture-tested but
have 0 live records** — none was ever added to `scrape.yml`'s crawl step
(issue #65, consolidated, priority:high). The remaining representativeness
gap is *source-count* (6 of 8 sources ever actually run; #65 closes it in
one PR) — topic coverage is no longer the binding constraint, and
government-category coverage now depends on a fresh `DIP_API_KEY` rather
than code. Ranked by yield per effort:

5. **CORE** (issue #94, shipped 2026-08-07, A38) **[done, code — see
   #65]** — third `studies`-table catalog source (`api.core.ac.uk`,
   no-key, no robots.txt exclusion). **Never actually run**: not in
   `scrape.yml`'s crawl step, 0 live records.
6. **Eurobarometer** (issue #35) **[done, code — see #65]** — shipped
   2026-07-15 as A24 (`study_scraper/sources/eurobarometer.py`, GESIS KG
   SPARQL filtered to Eurobarometer waves). **Never actually run**: not
   in `.github/workflows/scrape.yml`'s hardcoded source list, so it has
   0 live records.
7. **Bundestag DIP API** **[done 2026-08-03]** — `discovery/
   bundestag_dip.py`, catalog-style, fixture-tested; PR #91 switched to
   the no-key `bundestag.de/dip-api/api/v1` mirror after the hardcoded
   public API key started 401ing (#48). Live 2026-08-10: 1,115 studies
   across all 8 topics.
8. **GovData.de (CKAN)** (issue #64, shipped PR #69 2026-07-22)
   **[done, code — see #65]** — Germany's cross-government open-data
   catalog (federal + state + municipal metadata in one CKAN instance);
   free, no-auth `package_search` REST API, "Data License Germany 2.0"
   with per-dataset overrides captured explicitly. **Never actually
   run**: same gap as CORE/Eurobarometer.

### Scouted this round (2026-08-17)

- **Russia/Ukraine opinion data (API)** — searched specifically as an
  alternative path to #50's `russland_ukraine` topic ask (which is
  blocked on a `config/**` edit no agent can make). Found plenty of
  *reporting* (Statista — paywalled; bpb.de Ukraine-/Russland-Analysen,
  ARD-DeutschlandTrend via infratest dimap — HTML only, no API) but
  nothing structured/free. Confirms bpb.de stays a tier-3
  `SitemapSource` candidate (item 15), not a standalone win.
- **Destatis GENESIS re-verified** — an earlier stray search result
  implied "no registration needed"; checked the actual webservice docs
  and confirmed the REST/JSON API still requires a personal
  username+password or token (registration is free, but it's still a
  credential, not a truly keyless API). No change to its `needs-human`
  classification (item 11).
- Net: no new source module to propose — 4th consecutive scouting round
  (08-03, 08-10, 08-17 count as the same "nothing new" streak once you
  include the 07-20/07-27 pair before it) with no buildable find. The
  free/no-auth source frontier for this project looks exhausted for now;
  remaining candidates are all needs-human (credentials, items 11-13) or
  the tier-3 `SitemapSource` play (item 10).

### Scouted and rejected 2026-07-27/08-03

9. **BASE** **[needs-human — filed as #88, rejected 2026-08-03/05,
   closed]** — turned out not to be the OAI-PMH, no-auth source this
   assumed: BASE's only public interface is access-key/IP-allowlist
   gated (DECISIONS.md A35/A37). CORE filled the third `studies`-table
   source slot instead (A38).
- **DOAJ** **[rejected 2026-08-05]** — genuinely open, unauthenticated
  API, but `doaj.org/robots.txt` specifically disallows `ClaudeBot`
  (A37). Not building against a site that has opted this agent's
  crawler out.
9. **Domain-audit source discovery** (issue #38) **[done]** — Phase
   5d: `study_scraper sources-audit [--limit]` walks stored study/
   reference URLs, groups by registrable domain, and surfaces domains
   with no dedicated source, ranked by frequency (see DECISIONS.md A30).
   Dock/Streamlit surface shipped (issue #77): "Candidate sources" page
   reuses `audit_domains` verbatim. Plus: the product agent now
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
for our verification layers). All items below have shipped:

- **Fetch only what changed** **[done, #34/PR #47]** — conditional GET
  (`fulltext.py` sends `If-None-Match`/`If-Modified-Since` and treats a
  `304` response as unchanged) and content-hash fingerprints
  (`content_hash` stored and used for idempotent upserts in
  `storage/postgres.py`).
- **Politeness & backoff** **[done, #32]** — `study_scraper/http.py`'s
  shared `request_with_retry` gives every fetcher jittered exponential
  backoff and honours `Retry-After` on 429/503. `config.
  respect_robots_txt` is now actually enforced (self-propose,
  2026-08-10, DECISIONS.md A39) — was declared but never checked
  before; `fulltext.py`'s `fetch_url` now raises `RobotsDisallowed`
  when a host's robots.txt disallows the URL, scoped to that fetch
  loop only (see A39 for why the discovery source clients don't need
  it).
- **Two-layer bookkeeping** **[done]** — falls out of the two items
  above (etag/last-fetch/checksum metadata alongside the "already
  scheduled" keys).
- **Measure waste** **[done, #82/PR #83]** — `status --json` reports
  `duplicates_total`/`duplicate_rate` per run (crawl spend is visible;
  bytes-fetched-vs-kept was not added, not needed to close this item).
- **Config-driven crawling** **[done, #84/PR #85]** — topics flow from
  `topics.csv` into the scheduled crawl (2026-07-04) and the Eurostat
  dataset-code list is config-driven too.

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
  item 1) — landed 2026-07-19; item 2 (console Questions page, PR
  #60/#73) shipped by 2026-07-26 too. Only item 3 (lake→answers
  mapping proposal, design-first/Product-Direction-gated) remains
  open on #59.
- Retry-After cap (#53, PR #67), no-signal studies stop re-clogging the
  attribution queue (#68, PR #68, migration 0011), GovData.de source
  built (#64, PR #69 — but see #74, not wired into the crawl),
  `sources-audit` candidate-domain discovery (#38, PR #70), dock
  Questions page shows real last-digest shifts (PR #73) — all landed
  2026-07-20 through 2026-07-26.
- Dock "Candidate sources" page surfaces `sources-audit`'s unknown-domain
  ranking (#38 dock half, #77) — landed 2026-07-28.
- Topic-crawl rotation (#71, A32) — landed 2026-07-30, **confirmed working
  live 2026-08-03** (rente/verteidigung roughly doubled/tripled). Claims
  FK-violation-on-dedup fix (#79), crawl duplicate-rate metric (#82),
  Eurostat config-driven default codes (#84, A33) and JSON-stat typed
  projection (#86, A34, PR #87) — all landed 2026-07-29 through 08-02.
- `bundestag_dip` no-key mirror fix (#91, A35/A36) — landed 2026-08-03,
  confirmed live 2026-08-10 (1,115 studies, all 8 topics), then
  **regressed to 401 again 2026-08-13** (#106, A40 — the mirror itself
  started requiring auth; converges back to #48's original
  `DIP_API_KEY` ask, no further code fix available). CORE catalog
  source (#94, A38) — landed 2026-08-07. Both closed `GOAL.md`'s
  topic-coverage bar and it stays closed regardless of the regression;
  #65 (consolidated core+eurobarometer+govdata wiring) is the one
  remaining gap on the source-coverage bar.
- GESIS/Eurobarometer SPARQL retry/backoff (#108, A41) — landed
  2026-08-14. `status.py` attribution-staleness signal (#110, new,
  priority:high) — filed 2026-08-17 after the attribution pipeline went
  dark for 6+ days with no automatic signal (see #49).
- SSOAR 8/8-topic outage (#100) — confirmed transient upstream, no
  repeat on the 2026-08-13 or 2026-08-17 scheduled runs; closed
  2026-08-18.
- `status.py` per-source crawl staleness (#115) — landed 2026-08-20.
  Attribution run yield rate (#119, self-proposed) — `status` now
  surfaces the most recent run's found/attempts alongside the existing
  staleness signal, so the low/volatile "found" rate #49's monitor
  updates kept hand-computing from Postgres (e.g. 08-18: 0/40, 08-21:
  13/40) is visible without a manual query.
