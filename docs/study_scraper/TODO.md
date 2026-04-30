# TODO — ordered backlog

Work flows top-to-bottom. **Do not skip ahead** without updating this file
and `STATUS.md`. Items marked **[BLOCKED]** require a `DECISIONS.md` answer
before they start.

## Phase 0 — Knowledge base bootstrap (this commit)

- [x] Create `docs/study_scraper/` and write `README.md`, `GOAL.md`,
      `STATUS.md`, `DECISIONS.md`, `TODO.md`.
- [x] Document the broken Scrapy scaffold finding.
- [x] Surface design decisions Q1–Q10 to the maintainer.

## Phase 1 — Get unblocked

- [ ] **[BLOCKED on Q1–Q10]** Maintainer answers the open questions. The
      answers determine the rest of this list, so don't start Phase 2 until
      they are accepted.
- [ ] Once answered, move resolved Q's into the "Accepted" section of
      `DECISIONS.md` with date + resolution. Update this TODO.

## Phase 2 — Project skeleton

- [ ] Create `study_scraper/` package: `__init__.py`, `cli.py`, `config.py`,
      `models.py`, `storage/`, `discovery/`, `extraction/`, `pipeline.py`,
      `eval/`. Include a `study_scraper/README.md` that points back to this
      knowledge base.
- [ ] Add `study_scraper` to `pyproject.toml` packages and pin the minimal
      dependency set (httpx, pydantic, pyyaml, beautifulsoup4, pdfminer.six
      or pypdf, duckdb, structlog, tenacity, typer, plus embeddings library
      pending Q7).
- [ ] Add a `Makefile` target: `make scrape TOPIC=<id>`.
- [ ] Add a smoke test that imports the package and runs `cli.py --help`.

## Phase 3 — Topics + data model

- [ ] Implement topics loader per Q1's resolution. Emit a normalized
      `Topic` Pydantic model.
- [ ] Define `Study` Pydantic model: `id (hash of canonical_url),
      canonical_url, source_urls[], title, authors[], publisher,
      publication_date, language, topic_ids[], topic_scores{topic_id:
      float}, abstract, key_findings[], survey_metadata?, raw_artifact_ref,
      fetched_at, source_id, provenance{}`.
- [ ] Implement deduplication key strategy (canonical URL + DOI fallback +
      title-near-duplicate detection).

## Phase 4 — One source end-to-end

- [ ] Implement the **first** discovery source (likely GESIS SSOAR per Q4)
      as a single class implementing a `DiscoverySource` interface:
      `iter_candidates(topic) -> Iterable[Candidate]`.
- [ ] Implement an HTML fetcher with rate limiting, retries, robots.txt
      respect.
- [ ] Implement a PDF fetcher + text extractor (best effort).
- [ ] Implement the rule-based topic filter (port matching logic from
      `pipeline/topic_classifier.py` into `study_scraper/topic_filter.py`,
      decoupled from ClickHouse).
- [ ] Persist `Study` rows + raw artifacts to DuckDB + filesystem.
- [ ] CLI: `study_scraper run --source ssoar --topic klima --limit 50`
      should produce records in DuckDB.
- [ ] Manual review of the first 50 results; record findings in
      `docs/study_scraper/notes/first-run-<date>.md`.

## Phase 5 — Eval harness

- [ ] Resolve Q8 (gold-set ownership). Create
      `docs/study_scraper/eval/gold/<topic_id>.yml`.
- [ ] Implement `study_scraper eval --topic <id>`: runs the pipeline,
      compares to gold set, writes a markdown report under
      `docs/study_scraper/eval/reports/`.
- [ ] Add the report to the PR template so every iteration ships with
      numbers.

## Phase 6 — Second + third source

- [ ] Add OpenAlex source (per Q4 tier 1).
- [ ] Add Bundestag publications source.
- [ ] Validate dedup across sources works (SSOAR + OpenAlex commonly index
      overlapping records).

## Phase 7 — Semantic relevance (pending Q7)

- [ ] If Q7 = yes: integrate a small multilingual sentence-embedding model.
      Stage 2 scoring on abstract / first-page text.
- [ ] Re-run eval; compare metrics vs rules-only baseline.

## Phase 8 — Iteration loop

- [ ] Tighten thresholds, add more topics, expand to tier 2 sources from Q4
      based on eval-driven priorities.
- [ ] Reach the success criteria in `GOAL.md`. Mark goal as reached, write
      a wrap-up note in `STATUS.md`, and discuss step 2 (data engineering
      to answer the policy questions) — that is a separate project.

---

## Anti-goals (don't do these without an explicit decision)

- Do not introduce Airflow/dbt/ClickHouse/Streamlit dependencies into
  `study_scraper/`.
- Do not extend `scraping/` — see `DECISIONS.md` A3.
- Do not scrape behind logins or paywalls.
- Do not add more than ~3 sources before the eval harness exists. Without
  measurement we can't tell if we're improving.
- Do not start a UI before the data is stable and useful.
