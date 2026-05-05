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
python -m study_scraper run --source ssoar --topic klima --limit 50  # Phase 4
```

## Hard rules

- No imports from `dags/`, `dbt_project/`, `streamlit_app/`, or
  `scraping/`. The legacy ELT is frozen, not borrowed from.
- Imports from `pipeline/topic_classifier.py` are allowed only via a thin
  abstraction; the goal is to **port** the matching rules, not depend on
  the legacy ClickHouse loader.
- New cross-cutting design choices go in `DECISIONS.md` first.
