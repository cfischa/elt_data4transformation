# `study_scraper/`

The new core of the project. Discovers and extracts relevant German-society
studies (representative surveys, polls, research with quantitative data)
for each topic in `config/topics/topics.csv`.

This package is the **independent project** described in
`docs/study_scraper/`. Read those docs first:

1. [`docs/study_scraper/GOAL.md`](../docs/study_scraper/GOAL.md) — what
   we're building and what "done" looks like.
2. [`docs/study_scraper/STATUS.md`](../docs/study_scraper/STATUS.md) — the
   current state.
3. [`docs/study_scraper/DECISIONS.md`](../docs/study_scraper/DECISIONS.md)
   — accepted decisions + open questions.
4. [`docs/study_scraper/TODO.md`](../docs/study_scraper/TODO.md) — phased
   backlog. Read top-to-bottom; don't skip ahead.

## Quick commands

```
python -m study_scraper --help
python -m study_scraper topics
python -m study_scraper migrate                                  # apply SQL
python -m study_scraper run --source ssoar    --topic klima --limit 50
python -m study_scraper run --source openalex --topic klima --limit 50
python -m study_scraper list --topic klima
python -m study_scraper status                                   # coverage overview
streamlit run study_scraper/console/Home.py                       # control dock
```

## Control dock

`streamlit run study_scraper/console/Home.py` boots the operator UI.
Two pages, two features (per `docs/study_scraper/DECISIONS.md` A11 —
Streamlit is allowed inside `console/` only):

- **Home** — status overview: total studies, per-topic / per-source
  coverage, recent crawl runs, keep rate, failures.
- **Topics** — edit a topic's keywords; the page shows in real time
  how many studies *already in the DB* would match the new definition,
  plus the diff against the saved topic. Saving writes back to
  `config/topics/topics.csv`; the next CLI run picks it up. The dock
  does **not** start an ingest.

## Reading the data

Studies, crawl runs, and the join table all live in the
**`study_scraper`** Postgres schema. The schema, indexes, and example
queries are documented in [`../docs/study_scraper/DATA.md`](../docs/study_scraper/DATA.md).

Quick peek without leaving the shell:

```bash
python -m study_scraper list --topic klima
psql "$POSTGRES_URL" -c "SELECT title, topic_scores FROM study_scraper.studies LIMIT 5"
```

## Hard rules

- No imports from `dags/`, `dbt_project/`, `streamlit_app/`, or
  `scraping/`. The legacy ELT is frozen, not borrowed from.
- Imports from `pipeline/topic_classifier.py` are allowed only via a thin
  abstraction; the goal is to **port** the matching rules, not depend on
  the legacy ClickHouse loader.
- New cross-cutting design choices go in `DECISIONS.md` first.
