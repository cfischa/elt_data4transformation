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

1. **Spot-check CLI** — `study_scraper audit --sample-size N`: print N
   random stored attributions with their `source_span`, `grounded`,
   `dup_count`, and source URL, for weekly manual accuracy review
   (ACCURACY.md measurement C). Storage helper + CLI + test.
2. **Surface provenance in the dock Attributions page** — show
   `source_span` (the verbatim quote), a `grounded` ✓/✗ badge, the
   `distribution_check` flag, and `dup_count`. Console-only; page-compile
   test. Makes accuracy auditable by eye.
3. **HTTP robustness** — wrap the source HTTP clients (ssoar, openalex,
   fulltext fetch) in `tenacity` retry + exponential backoff, and honour
   HTTP 429. One small helper reused across sources; unit-test the retry
   policy with a mock transport. Production resilience.
4. **Eurostat typed SQL view(s)** — a migration projecting the JSON-stat
   `env_air_gge` (and `nrg_bal_s`) lake payloads into typed rows
   (geo, year, value), like the existing `dawum_poll_results` view, so the
   numbers are queryable. Migration + a view smoke check.

## P2 — next (still [now])

5. **BASE source** — OAI-PMH academic index; reuse the SSOAR OAI parser,
   new source id `base`. Fixture + unit tests (no live call in CI).
6. **F2 attribution → claim lineage** — add `claim_id` FK on
   `attributions` (migration) and thread the claim id through so each
   triple links to the exact claim row (deeper than `source_span`).
7. **Distribution-warning surfacing** — in `ask` / the dock, flag findings
   whose `raw.distribution_check is False` so impossible splits are visible.
8. **Domain-audit discovery (Phase 5d)** — from stored study URLs, group
   by domain and list domains we have no source for as candidate sources
   (read-only report + a dock list). Pure aggregation; testable.

## P3 — needs the maintainer ([needs-human])

9. **Destatis GENESIS source** — REST API, free registration. Scaffold +
   `from_file` tests buildable now; live needs a credential secret.
10. **UBA / BAMF structured downloads** — XLSX/CSV lake sources; need real
    sample files to pin the parser.
11. **Gold set + eval wiring** — replace the sample gold in
    `study_scraper/eval/gold/` with ~50 curated studies + ~40 tagged
    titles, then the harness reports a real accuracy number.
12. **spaCy lemmatization** (full German morphology) and **OCR** for
    scanned PDFs — both need host installs.

## Done (recent)
- PDF resolver, dock Attributions + Sources pages, auto-reviewer,
  cross-finding dedup, accuracy F1/F3/F4/F5/F6, eval harness scaffold,
  `atomkraft` topic. See ACCURACY.md / TODO.md.
