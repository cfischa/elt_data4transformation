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

Updated 2026-09-21: Studies: **7,467** (+61 since 09-14, +0.8% — growth
still nearly flat, entirely from `openalex` (5,381) + `ssoar` (971);
`bundestag_dip` now 42.3 days stale, still 401ing (#113)). **#145
confirmed fixed and closed**: PR #165 (lazy `bs4` import) merged 09-14,
`scheduled-attribute` has run successfully several times since,
attribution queue backlog is draining (233 → 204), last run yield 8/40
(20%). The answering half of the goal is dark no longer, just slow.
**Filed #172** (priority:med, needs-human): reference-follower's
`follow --fetch` (#136, shipped 08-31) still isn't in `scrape.yml`'s
crawl step — 154,375 pending candidates, 0 ever fetched, flagged as
"will file next round" on 08-31 and 09-14, now an actual issue.
Source-coverage math worth stating plainly: **7 of the 8
`GOAL.md`-required production sources have run** (`openalex`, `ssoar`,
`bundestag_dip`, `dawum`, `gesis`, `eurostat`, `bmas`); `core`,
`eurobarometer`, `govdata` are built and tested but stuck at 0 live
records purely because `scrape.yml` never got the 3 lines (#65,
6+ weeks open) — the single closest the project has been to clearing
this primary bar without any new engineering. Scouted Russia/Ukraine
and Wohnen/Miete opinion data this round (8th straight round, nothing
new buildable — see the Product Direction issue for detail).

1. **You: paste #65's 3 lines into `scrape.yml`.** Cheapest, highest-
   leverage action on the board — closes `GOAL.md`'s ≥8-production-
   sources bar outright (7/8 → 10/8), no secrets, no design call.
2. **Wire `follow --fetch` into `scrape.yml`** (issue #172, new,
   priority:med, needs-human) — code is done (#136), just needs a
   crawl-step addition; 154k-candidate backlog otherwise sits idle
   indefinitely.
3. **Attribution throughput** (issue #49) **[needs-human]** — no
   longer fully dark, but still the starkest number in the project
   (0.9% of claims ever attributed); the fixed `--limit 40` cadence in
   `.github/workflows/attribute.yml` is the next binding constraint
   once the post-outage backlog finishes draining.
4. **Topic-content gap** (issue #50, priority:high, open since
   2026-06-26 — 13+ weeks) **[needs-human]** — maintainer's ask
   (Erbschaftssteuer keywords on `steuern`, new `russland_ukraine`
   topic) needs `config/topics/topics.csv` + `questions.yml`, both
   outside `study_scraper/**`/`tests/**`/`docs/study_scraper/**`, so
   neither agent can build it as scoped.
5. **`bundestag_dip`** (issue #48, reopened in spirit as #106/#113/A40)
   — still 401ing as of 2026-09-21 (42.3 days stale). No further code
   fix available; converges back to needing a real `DIP_API_KEY`
   (`infoline.id3@bundestag.de`). Deprioritized below #65 per
   maintainer's 08-24 silent-default.
6. **Eurostat typed projection** (issue #86) **[done 2026-08-02]** —
   `study_scraper/jsonstat.py::flatten_jsonstat` decodes the JSON-stat
   `id`/`size`/`dimension`/`value` encoding into typed rows (dimension
   labels + value); `eurostat-table --code <code>` is the queryable
   surface. Python, not a SQL view — see DECISIONS.md A34 for why.
7. **OpenAlex 429s structurally starving `rente`/`verteidigung`**
   (issue #71) **[done 2026-07-30]** — topic-crawl order now rotates by
   `GITHUB_RUN_NUMBER` (A32). Confirmed working live 2026-08-03: both
   topics' openalex counts roughly doubled/tripled week over week.
8. ~~SSOAR outage, 8/8 topics failed 2026-08-10~~ **[done/closed, issue
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

Current coverage (2026-09-21 live DB): **catalog** OpenAlex (5,381) +
SSOAR (971), and **Bundestag DIP (1,115, stalled since 08-10 — 401ing
again, #113/A40, now 42.3 days stale)** — academic + government, three
sources have fed the `studies` table (GOAL.md's topic-coverage bar is
met and stays met, see P1 intro); **lake** DAWUM (vote intention, 3,956
rows), GESIS KG (survey catalog, 500), Eurostat (official stats, 3 —
thin by design), BMAS (pension statistics, 2). **CORE, Eurobarometer,
GovData.de are all built and fixture-tested but have 0 live records** —
none was ever added to `scrape.yml`'s crawl step (issue #65,
consolidated, priority:high). The remaining representativeness gap is
*source-count* (7 of 8 sources ever actually run; #65 closes it in one
PR, taking it to 10) — topic coverage is no longer the binding
constraint, and government-category coverage now depends on a fresh
`DIP_API_KEY` rather than code. Ranked by yield per effort:

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

### Scouted this round (2026-09-21)

- Searched Russia/Ukraine-war opinion data (ties to the maintainer's own
  06-26 `russland_ukraine` topic ask, #50, still open) and Wohnen/Miete
  opinion data (housing satisfaction / rent-level sentiment). Both
  dead ends: Ukraine-war polling is Statista (paywalled) or bpb.de's
  Ukraine-Analysen digest (HTML summary of others' polls, no API —
  already tracked as a tier-3 `SitemapSource` candidate). Housing
  opinion data is the same shape: Statista paywalled, Destatis publishes
  official rent statistics (indicators) not opinion percentages.
- Net: **8th consecutive scouting round with no new free/no-auth
  opinion source** across the full 8-topic list (every topic has now
  had at least one dedicated pass since 07-20). The frontier for the
  current topic set looks exhausted; further scouting will likely need
  to look beyond these 8 topics, or revisit the tier-3 `SitemapSource`
  work (bpb.de, eupinions, polling-firm press releases) as the next
  real lever for topics like Russia/Ukraine and climate that keep
  surfacing HTML-only candidates.

### Scouted this round (2026-09-14)

- Searched specifically on `klima` + `atomkraft` (the maintainer's
  original headline topic, and one under-scouted vs.
  rente/verteidigung/wohnen/bildung/steuern/migration in recent rounds).
  **PACE-Studie** (Uni Erfurt's recurring "Planetary Health Action
  Survey", ~1,000 respondents, tracks German climate-crisis attitudes) is
  a genuine, real, ongoing survey — but publishes only via its own site
  (`pace-studie.de`) and press releases, no API or CSV download found.
  Same tier-3 HTML bucket as bpb.de/eupinions. Atomkraft searches
  surfaced only Statista (paywalled) and press-release polls (Innofact/
  Verivox, already ingested via the abstract/claims path where cited by
  academic sources) — nothing structured/new.
- Net: no new buildable source this round — the free/no-auth
  opinion-data frontier for this topic list remains exhausted across all
  8 topics now (every topic has had at least one dedicated scouting pass
  since 07-20 with nothing new found).
- Closed two dead-end candidates from last round after investigation:
  **#150** (Eurobarometer→attributions) — its GESIS catalog harvest is
  metadata only, no toplines anywhere in the payload, real percentages
  need login-gated microdata. **#151** (BMF Datenportal) —
  `bundesfinanzministerium.de` is Radware-bot-gated, and the same
  datasets are already reachable via the existing `govdata` source
  (BMF's GovData org).

### Scouted 2026-09-07

- **Bundesfinanzministerium Datenportal** — filed as issue #151
  (`priority:low`), **closed 2026-09-14** — see above, turned out
  bot-gated and redundant with `govdata`.
- Searched specifically on `steuern` and `migration_einwanderung` (both
  under-scouted in recent rounds vs. rente/verteidigung/wohnen/bildung).
  Migration search surfaced nothing new: Statista (paywalled), YouGov DE
  and FES (HTML/PDF only, no API) — same dead ends as prior rounds.
- Net: one new low-priority buildable candidate this round, same
  indicator-not-opinion pattern as BMAS — the free/no-auth
  *opinion-data* frontier is still exhausted; official-statistics
  sources remain the only fresh ground.

### Scouted 2026-08-31

- **BMAS pension statistics (Rentenbestandsstatistik)** — new candidate,
  filed as issue #137 (`priority:low`). Free, no-auth CSV/XLSX downloads
  at bmas.de, yearly since 2014, per-reporting-date CSVs since the
  2021-07-01 revision. Official indicator data (recipient counts/amounts),
  not opinion data, and no explicit license stated on the page — flagged
  as caveats in the issue. Useful mainly for `rente` topic-coverage depth,
  not for the answering/attribution layer.
- **European Social Survey (ESS)** — re-confirmed registration-gated (same
  shape as GESIS microdata/Q19); no change to its needs-human status.
- **OParl** (German municipal council open-data standard) — real and
  free, but wrong shape: per-municipality session/document/resolution
  data, not opinion or study content, and fragmented across hundreds of
  independent endpoints. Not a fit for this project's source model.
- First scouting round in 6 with a new buildable candidate (BMAS), though
  a low-priority one — the free/no-auth *opinion-data* frontier still
  looks exhausted; remaining upside is in adjacent indicator/statistics
  sources like this one, or the needs-human tiers already tracked.

### Scouted 2026-08-24

- **Bildung (education) opinion/structured data** — searched for a
  German open-data API on public education attitudes/outcomes.
  Found only the Destatis-GENESIS-based "Kommunale Bildungsdatenbank"
  (municipal education statistics) — same `needs-human` registration
  gate as GENESIS generally (item 11), and indicators, not opinion
  data. No new candidate.
- **Wohnen (housing) opinion/structured data** — searched for a German
  open API on housing-market sentiment/rent opinion. Found only
  Destatis Mikrozensus housing tables (official stats, not opinion)
  and paywalled Statista/Ipsos summaries. No new candidate.
- Net: **5th consecutive scouting round with no new buildable source**
  (08-03, 08-10, 08-17, 08-24, plus the 07-20/07-27 pair). The free/
  no-auth frontier for this project's topic list looks genuinely
  exhausted — remaining candidates are all `needs-human` (GENESIS
  registration, GESIS microdata licences) or the tier-3 `SitemapSource`
  play (item 10). Future scouting rounds should consider widening
  beyond the 8 existing topics' exact keywords rather than re-querying
  the same ground.

### Scouted 2026-08-17

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
17. **BMAS Rentenbestandsstatistik / social-policy statistics** (scouted
    2026-08-31, issue #137) **[now, low priority]** — free, no-auth
    CSV/XLSX downloads on `bmas.de` (yearly since 2014, per-date CSVs
    since 2021-07-01). Official pension-statistics indicators, not
    opinion data; license unstated on the page (capture per-record,
    same pattern as A29's GovData.de). Buildable now as a lake source
    (A14 shape); low priority since `rente`'s source-coverage bar is
    already met and this doesn't feed the answering layer.

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
- Flaky date-unpinned test fix (#117) — landed 2026-08-20. Attribution
  yield metric COUNT-vs-SUM bug (#121, caught the day after #119
  shipped) — fixed 2026-08-23, `status` now reports `sum(found)`
  correctly (was undercounting, e.g. 5/40 instead of the true 13/40 for
  08-21). Console Sources page kind-column misclassification for
  bundestag_dip/core/govdata/eurobarometer (#123) — fixed 2026-08-23,
  all 9 sources now classify correctly.
- Console Home dashboard surfaces attribution + crawl staleness (#127) —
  landed 2026-08-25. Attribution queue backlog size (#128, self-proposed)
  — `status`/dock now show `attribution_queue`'s row count directly, so
  #49's monitor updates no longer need to hand-query Postgres for it.
- Zero-yield run streak surfaced in status/dock (#130) — landed
  2026-08-2x, closing out the attribution-visibility line of work
  (staleness/yield/backlog/streak are all now surfaced). #49's actual
  cadence fix still needs a `.github/workflows/attribute.yml` edit —
  see P1 item 3.
- Filed #136 (reference-follower step 2, Phase 5d) as the next coverage
  lever now that `openalex`/`ssoar` growth is slowing under rate limits
  and the three needs-human blockers (#65/#113/#49) haven't moved since
  07-04. Scouted and filed #137 (BMAS pension statistics, low priority)
  — 2026-08-31.
- #136 shipped (2026-08-31) plus its follow-ups: candidate-studies queue
  in the dock (#141/#142), citation-graph topic-scoping fix (#143/#144,
  A44), BMAS lake source (#137/#140), and `KNOWN_DOMAINS` coverage for
  `core.ac.uk`/`bmas.de` (#146/#147). Self-proposed #148 next (2026-09-06):
  `status`/dock had a queue for the follower's *pending* candidates but
  no signal for whether it had ever actually been run — added
  `reference_follower_studies_total`, same "surface X" pattern as
  #110/#115/#119/#128/#130/#132.
- **2026-09-07 (Product Direction):** live `reference_follower_studies_total`
  reads **0** — #136 has never actually run in production (not wired into
  `scrape.yml`, needs-human, flagged for the first time this round). Bumped
  #145 (`scheduled-attribute` hard-broken by a missing `beautifulsoup4` in
  `attribute.yml`'s install step, filed by the monitor 2026-09-04) to
  `priority:high` — it's the acute blocker this week, distinct from #49's
  slow-cadence problem. Resolved #59 item 3's design gate (lake→answers):
  Eurobarometer-only adapter, filed as #150. Closed #59. Scouted and filed
  #151 (Bundesfinanzministerium Datenportal, low priority).
- #156 shipped (2026-09-08): `status`/dock now surfaces `never_run_sources`
  (known source_ids with zero `crawl_runs` rows), closing the "shipped-but-
  idle looks identical to doesn't-exist" gap behind #65/#148. Investigated
  #150 and #151 before building: both turned out not buildable as scoped
  (Eurobarometer's GESIS KG harvest is catalog metadata only, no toplines
  to map; BMF Datenportal is Radware-bot-gated and redundant with GovData's
  existing BMF-org harvest) — left `needs-human` with the investigation
  recorded on each issue rather than shipping a no-op/fabricated adapter.
- Self-proposed #157 (2026-09-09): with every open `agent:task` issue
  already `needs-human` (secrets/`.github/**`/config edits, or the #150/
  #151 dead ends above), self-proposed from the "surface X" vein again:
  `status`/dock had a single global `attribution_queue_size` (#128) but no
  breakdown of which topic the backlog was concentrated in — added
  `attribution_queue_per_topic` (`status` text + dock Home expander), same
  pattern as #110/#115/#119/#128/#130/#132/#148/#156.
- Self-proposed #161 (2026-09-12): same "surfaced in one place, invisible
  in the other" gap, dock-side this time — the CLI's `status` text has
  shown kept/pending/rejected study counts since Q12's review-queue work,
  but the dock's Home page only ever showed the `total_studies` sum, so a
  dock-only operator had no landing-page signal that a review-queue
  backlog (`2_Review.py`) existed at all. Added the breakdown to the
  `Studies` metric's tooltip plus a callout when `pending_count > 0`. No
  new backend field — `pending_count`/`rejected_count`/`kept_count`
  already existed and were tested.
- **2026-09-14 (Product Direction):** root-caused #145 (attribution
  pipeline down since 09-01) to `cli.py`'s eager, unguarded `BMASSource` →
  `bs4` import chain and filed the fix as **#163** (priority:high,
  agent-buildable, no `.github/**` edit needed — see P1 intro). Closed
  #150 and #151 as investigated dead ends. Scouted `klima`/`atomkraft`
  (PACE-Studie, Innofact/Verivox press polls) — no new buildable source.
