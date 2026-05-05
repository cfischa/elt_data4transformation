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

## Phase 3 — Data model + storage layer

- [ ] Implement `Topic` Pydantic model + CSV loader.
- [ ] Implement `Study` Pydantic model: `id (sha256 of canonical_url),
      canonical_url, source_urls[], title, authors[], publisher,
      publication_date, language, topic_ids[], topic_scores{topic_id:
      float}, has_quantitative_data, abstract, key_findings[],
      survey_metadata?, raw_artifact_ref, fetched_at, source_id,
      provenance{}`.
- [ ] Write SQL migration files under `study_scraper/migrations/`
      (Postgres-compatible, valid as Supabase migrations).
- [ ] Storage adapter targeting Supabase (Postgres + Storage). Local-dev
      mode reads `SUPABASE_URL`/`SUPABASE_SERVICE_KEY` from env; falls
      back to a plain Postgres URL when those are absent so unit tests
      can run against a Docker container.
- [ ] Deduplication: canonical-URL hash + DOI fallback + title-near-
      duplicate (rapidfuzz, threshold tunable).

## Phase 4 — One source end-to-end

- [ ] Implement `DiscoverySource` interface in `study_scraper/discovery/`.
- [ ] First source: **GESIS SSOAR** (`iter_candidates(topic) ->
      AsyncIterator[Candidate]`).
- [ ] HTTP fetcher: rate-limited, retried (tenacity), robots.txt-aware.
- [ ] PDF text extractor (pypdf), HTML extractor (BeautifulSoup +
      readability heuristics).
- [ ] Stage-1 topic filter: port the matching logic from
      `pipeline/topic_classifier.py` into
      `study_scraper/topic_filter.py`, decoupled from ClickHouse.
- [ ] Persist `Study` rows + raw artifacts (Supabase Storage; local-fs
      fallback when storage credentials absent).
- [ ] CLI: `python -m study_scraper run --source ssoar --topic klima
      --limit 50` should produce records.
- [ ] Manual review of first 50 results; record findings in
      `docs/study_scraper/notes/first-run-<date>.md`.

## Phase 5 — Second + third source

- [ ] Add OpenAlex source (per A6).
- [ ] Add DAWUM source (port the existing legacy connector behind the
      `DiscoverySource` interface; do not import the legacy ClickHouse
      loader).
- [ ] Add Bundestag publications source.
- [ ] Validate dedup across sources.

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
