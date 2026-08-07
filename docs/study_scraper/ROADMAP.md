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

Re-prioritized 2026-08-03: #71 (OpenAlex 429s starving `rente`/
`verteidigung`) shipped and is confirmed working live — both topics
roughly doubled/tripled (285→590, 143→521) and overall studies grew
+15.8% this week, the best growth since the 07-13 jump. With that lever
pulled, the next-biggest in-scope gap is structural, not a bugfix: only
2 of the 5+ built sources feed the `studies` table GOAL.md's
topic-coverage criterion counts. See issue #8 for the live metrics
behind this reorder.

1. **Third catalog source for `studies`** (issue #88, priority:high)
   **[done 2026-08-07]** — BASE (needs-human, access-key gated,
   DECISIONS.md A35) and DOAJ (rejected, `robots.txt` disallows
   `ClaudeBot`, A37) both dead-ended. **CORE** (`api.core.ac.uk`)
   verified live 2026-08-07: real, no-key `200` JSON responses
   (title/abstract/authors/DOI/dates; only full text withheld for
   anonymous callers), no `robots.txt` disallow. Shipped
   `study_scraper/discovery/core_search.py` (issue #94, DECISIONS.md
   A38), fixture-tested, `run --source core --topic <id>`.
   `GOAL.md`'s ≥3-sources topic-coverage bar is now buildable once
   wired into a scheduled crawl (same maintainer-follow-up gap as
   Eurobarometer/GovData, #65/#74).
2. **`bundestag.de` Open-Data XML dumps as a no-key DIP alternative**
   (issue #89, priority:med) **[done 2026-08-03 — via a different fix,
   issue left open]** — investigated as scoped; found something better
   along the way (`bundestag.de/dip-api/api/v1`, a no-key mirror of the
   *same* DIP backend `bundestag_dip.py` already calls) and shipped that
   instead of a new bulk-dump source. See DECISIONS.md A35 and PR #91
   (merged 2026-08-03, closed #48). #89 itself was never closed since
   PR #91 only referenced `Closes #48` — no remaining work here.
3. **Attribution throughput** (issue #49) **[needs-human]** — `claims`
   is at 7,110 rows and climbing every crawl; `attributions` crept from
   57 (07-21) to only **71** (08-03) — 13 days, ~1/day. The #68
   no-signal fix keeps the queue itself healthy (358, draining on
   zero-yield runs), so the only remaining lever is
   `scheduled-attribute`'s cadence/`--limit` in
   `.github/workflows/attribute.yml`, out of both agents' edit scope.
   Still the single biggest lever on the *answering* half of the goal —
   3 straight weekly updates with no movement.
4. **`bundestag_dip` still fully broken** (issue #48, reopened
   2026-07-20) **[needs-human]** — 64/64 runs ever still 401, 0 studies
   ever. Only pure-government catalog source via the REST API; needs a
   fresh `DIP_API_KEY` (free, mail to infoline.id3@bundestag.de) set as
   a repo secret. See item 2 for a no-key hedge being explored in
   parallel.
5. **Topic-content gap** (issue #50, priority:high, open since
   2026-06-26 — 5+ weeks) **[needs-human]** — maintainer's ask
   (Erbschaftssteuer keywords on `steuern`, new `russland_ukraine`
   topic) needs `config/topics/topics.csv` + `questions.yml`, both
   outside `study_scraper/**`/`tests/**`/`docs/study_scraper/**`, so
   neither agent can build it as scoped.
6. **Two built sources stuck at 0 live records, same root cause**
   — Eurobarometer (issue #65, open since 07-20) and **GovData.de**
   (issue #74, open since 07-27, code shipped in PR #69 on 07-22).
   Both just need one line each added to `.github/workflows/
   scrape.yml`'s crawl step — out of agent edit scope. GovData is
   worth prioritizing once actioned: as a whole-of-government CKAN
   catalog it may surface structured Rentenversicherung/Bundeswehr
   datasets that would help the two weakest topics.
7. **Eurostat typed projection** (issue #86) **[done 2026-08-02]** —
   `study_scraper/jsonstat.py::flatten_jsonstat` decodes the JSON-stat
   `id`/`size`/`dimension`/`value` encoding into typed rows (dimension
   labels + value); `eurostat-table --code <code>` is the queryable
   surface. Python, not a SQL view — see DECISIONS.md A34 for why.
8. **OpenAlex 429s structurally starving `rente`/`verteidigung`**
   (issue #71) **[done 2026-07-30]** — topic-crawl order now rotates by
   `GITHUB_RUN_NUMBER` (A32). Confirmed working live 2026-08-03: both
   topics' openalex counts roughly doubled/tripled week over week.

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

Current coverage (2026-08-03 live DB): **catalog** OpenAlex (5,266) +
SSOAR (528) — academic, openalex dominates volume, both still the
*only two* sources feeding the `studies` table (see P1 item 1); **lake**
DAWUM (vote intention, 3,886 rows), GESIS KG (survey catalog, 500),
Eurostat (official stats, 3 — thin by design), **Eurobarometer (0 —
built, never run, see #65) and GovData.de (0 — built, never run, see
#74)**. **Bundestag DIP is wired but has 0 studies ever** — see #48
(reopened, 64/64 runs ever still 401; #89 explores a no-key
alternative). The representativeness gaps are *government coverage*
(the one category with zero live sources despite two built) and
*catalog-source diversity* (only openalex+ssoar count toward
GOAL.md's ≥3-sources-per-topic bar; see #88). Ranked by yield per
effort:

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
   (free by mail to infoline.id3@bundestag.de). #89 explores
   `bundestag.de/services/opendata`'s no-key XML dumps as a hedge in
   parallel.
7. **GovData.de (CKAN)** (issue #64, shipped PR #69 2026-07-22)
   **[done, code — but see #74, new 2026-07-27]** — Germany's
   cross-government open-data catalog (federal + state + municipal
   metadata in one CKAN instance); free, no-auth `package_search` REST
   API, "Data License Germany 2.0" with per-dataset overrides captured
   explicitly. **Never actually run**: same gap as Eurobarometer — not
   in `scrape.yml`'s crawl step, 0 live records 5 days after shipping.
   Once wired, may also help close the `rente`/`verteidigung` coverage
   gap (item 1 in P1) by surfacing Rentenversicherung/Bundeswehr-
   adjacent government datasets without a bespoke scraper.

### Scouted this round (2026-08-03)

- **`bundestag.de/services/opendata`** — Plenarprotokolle +
  Drucksachen from the 1st electoral period onward, as XML/JSON
  **file downloads, no API key**. Different shape than the DIP REST
  API (bulk dumps vs. a searchable query) but no maintainer action
  needed to start. **Filed as #89** — a research spike, not a
  guaranteed ship (bulk size vs. topic-scoped filtering needs
  checking first).
- **Deutsche Rentenversicherung / `statistikportal.de` Open Data**
  (re-checked, pension-specific) — publication downloads (PDF/XLSX,
  e.g. "Rentenversicherung in Zeitreihen") and a research-data center
  (FDZ-RV) that's account-gated for microdata. No standalone free API
  beyond what Destatis GENESIS already covers. Not newly actionable;
  same bucket as GENESIS (item 11, needs-human registration).
- **ZMSBw "Sicherheits- und verteidigungspolitisches Meinungsbild"**
  (re-checked, defense-specific) — still the same finding as 07-27:
  GESIS-archived (ZA7613) but microdata behind login. Not newly
  actionable.

### Scouted and rejected 2026-07-27

8. **BASE** **[needs-human — filed as #88, rejected 2026-08-03/05]** —
   turned out not to be the OAI-PMH, no-auth source this assumed:
   BASE's only public interface is access-key/IP-allowlist gated (see
   P1 item 1, DECISIONS.md A35/A37). CORE filled the third
   `studies`-table source slot instead — see P1 item 1, A38.
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
