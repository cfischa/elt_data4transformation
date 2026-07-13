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

Re-prioritized 2026-07-13: the raw-collection pipes (HTTP resilience,
CI artifact path, incremental harvesting) are done. The current
bottleneck is that collection has outpaced everything downstream of
it — attribution isn't keeping up, and one required source category
is silently producing zero records. See issue #8 for the live
metrics behind this reorder.

1. **Attribution throughput** (issue #49) **[now]** — `claims` is at
   4,369 rows and growing every crawl; only 33 attributions have ever
   been written (one 2026-07-11 run). `scheduled-attribute` runs
   twice-weekly at `--limit 20` studies — far below crawl volume.
   Change the cadence and/or batch size (e.g. daily, or size scaled to
   `crawl_runs` output) so the backlog trends toward zero instead of
   growing, and run a one-off catch-up pass. This is the single
   biggest lever: 2,593 collected studies are only as useful as the
   structured `(question, position, %)` triples we actually extract
   from them.
2. **`bundestag_dip` silent 401** (issue #48) **[now]** — every request
   has 401'd since 2026-07-06 on the hardcoded public API key; the
   failure doesn't increment `errors` so it looked like a clean
   `seen=0` row instead of a broken source. Fix: raise/count the
   error so a dead source is visible in `study_scraper status`
   (see #48 for the exact call-site). Getting a fresh `DIP_API_KEY` is
   a separate **[needs-human]** step (free, by mail to
   infoline.id3@bundestag.de) — file that need in the issue but ship
   the visibility fix regardless.
3. **Topic-content gap** (issue #50) **[now]** — maintainer's
   2026-06-26 ask (Erbschaftssteuer keywords on `steuern`, new
   `russland_ukraine` topic) never landed. Pure `topics.csv` +
   `questions.yml` change, no code risk.
4. **Eurostat typed SQL view(s)** — a migration projecting the JSON-stat
   `env_air_gge` (and `nrg_bal_s`) lake payloads into typed rows
   (geo, year, value), like `dawum_poll_results`, so the numbers are
   queryable. Still only 3 lake rows total (2 codes × geo=DE) so this
   stays lower-value until the code list grows — keep it after 1–3.

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

Current coverage: **catalog** SSOAR + OpenAlex (academic, 2,373 +
220 studies as of 2026-07-13 — openalex now dominates volume); **lake**
DAWUM (vote intention, 3,861 rows), GESIS KG (survey catalog, 500),
Eurostat (official stats, 3 — thin by design). **Bundestag DIP is
wired but currently producing zero records** — see #48. The
representativeness gaps are *issue-opinion* data and *official
statistics breadth*. Ranked by yield per effort:

5. **Eurobarometer** (issue #35) **[now]** — the EU Commission's
   standing opinion survey; free, structured, includes Germany, directly
   answers "what do people think about X". Lake source; data via the EC
   open-data portal / GESIS mirrors. Bonus: Eurobarometer waves
   regularly poll German attitudes on the Russia/Ukraine war and
   sanctions, so this also feeds the new `russland_ukraine` topic
   (#50) with real structured data, not just a keyword filter.
6. **Bundestag DIP API** **[broken since 2026-07-06, see #48]** —
   `discovery/bundestag_dip.py`, catalog-style, fixture-tested; in the
   scheduled crawl, but the hardcoded public API key now 401s on every
   request (0 records ingested). Needs a fresh personal key (free by
   mail to infoline.id3@bundestag.de) **and** the silent-failure fix
   in #48 so a dead source shows up as `errors>0`.
7. **BASE** **[now]** — OAI-PMH academic aggregator (Bielefeld);
   reuses the SSOAR OAI parser almost verbatim; widens the catalog far
   beyond SSOAR. Fixture + unit tests, no live call in CI.
8. **Domain-audit source discovery** (issue #38) **[now]** — Phase 5d:
   walk stored study/reference URLs, group unknown domains, surface them
   as candidate sources (report + dock list). Plus: the product agent now
   scouts via WebSearch each run.
9. **Polling-institute press releases** **[now, larger]** — Forsa, INSA,
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
13. **bpb.de "Ukraine-/Länder-Analysen"** (Forschungsstelle Osteuropa
    Bremen, scouted 2026-07-13) — free, no auth, periodic HTML analysis
    series that regularly cites representative war-opinion surveys
    (German and Ukrainian). Tier-3 (HTML, no API) — candidate for the
    `SitemapSource` config (item 9) once that tier is built, not
    buildable standalone.
14. **eupinions** (Bertelsmann Stiftung quarterly EU opinion survey,
    scouted 2026-07-13) — same tier-3 bucket as #13; blog/PDF
    publication, no API found. Lower priority than #13 — Bertelsmann is
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
