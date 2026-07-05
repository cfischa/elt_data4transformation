# elt_data4transformation

This repository contains two coexisting projects:

1. **`study_scraper/` — the active project.** A study scraper for
   German political topics. Coverage-first (per
   [`docs/study_scraper/DECISIONS.md`](docs/study_scraper/DECISIONS.md)
   A12). Lake-style structured-data ingestion (DAWUM, GESIS, …) plus
   a topic-filtered academic catalog (SSOAR, OpenAlex), with a
   Streamlit operator dock for review.
   → **[Read `docs/study_scraper/README.md`](study_scraper/README.md)**
   first; then `GOAL.md`, `STATUS.md`, `DECISIONS.md`.

2. **Legacy ELT (`connectors/`, `dags/`, `dbt_project/`, `elt/`,
   `pipeline/`, `streamlit_app/`).** Frozen since the pivot
   (decision A1, 2026-04-30). Not deleted — kept for reference and
   for the bits `study_scraper` lifted (taxonomy schema, classifier
   rules, the base async connector pattern). Do not extend it.

The original ELT-pipeline README sat at this path. It described a
9-container Airflow + ClickHouse + dbt + Streamlit stack that drifted
from the original PRD's intent — *"scrape the internet for
representative German policy polls"*. The study scraper is the
catch-up to that intent.

## Quick start (study scraper)

```bash
# Local Postgres for the scraper (independent of the legacy stack)
docker compose -f study_scraper/docker-compose.yml up -d

export POSTGRES_URL=postgresql://postgres:postgres@localhost:5544/study_scraper
python -m study_scraper migrate

# Catalog-style sources (topic-filtered; write to `studies`)
python -m study_scraper run    --source ssoar    --topic klima
python -m study_scraper run    --source openalex --topic klima

# Lake-style sources (raw payload; write to `source_records`)
python -m study_scraper ingest --source dawum    --topic klima
python -m study_scraper ingest --source gesis    --topic klima   # see docs

# Query the data
python -m study_scraper search klimaschutzgesetz   # extracted claims
python -m study_scraper ask    klimaschutzgesetz   # structured findings (llm-v1)
python -m study_scraper answer atomkraft           # poll-of-polls aggregate
python -m study_scraper view   dawum_poll_results   # SQL view over the lake
python -m study_scraper status

# Products over the answer layer (2026-07-05)
python -m study_scraper watch add klimagesetz --label "Climate law"
python -m study_scraper digest --out digest.md      # shifts + novelties per watch
python -m study_scraper dossier atomkraft --out dossier.md   # citable report
python -m study_scraper gaps                        # evidence-gap table per topic
python -m study_scraper policy-gap --topic klima    # opinion vs Bundestag DIP
python -m study_scraper export --out dataset/       # open CSV dataset

# Streamlit operator dock (status overview + topic editor + review queue)
streamlit run study_scraper/console/Home.py
```

## Where to read further

| File | Purpose |
| --- | --- |
| [`docs/study_scraper/RUNBOOK.md`](docs/study_scraper/RUNBOOK.md) | **Production runbook — start here when returning.** |
| [`study_scraper/README.md`](study_scraper/README.md) | Package overview + hard rules. |
| [`docs/study_scraper/GOAL.md`](docs/study_scraper/GOAL.md) | What we're building and what "done" looks like. |
| [`docs/study_scraper/STATUS.md`](docs/study_scraper/STATUS.md) | Current state. |
| [`docs/study_scraper/DECISIONS.md`](docs/study_scraper/DECISIONS.md) | Accepted decisions A1–A14 and open questions. |
| [`docs/study_scraper/TODO.md`](docs/study_scraper/TODO.md) | Phased backlog. |
| [`docs/study_scraper/DATA.md`](docs/study_scraper/DATA.md) | Schema, example row, common queries. |
| [`docs/study_scraper/notes/`](docs/study_scraper/notes/) | Per-run notes incl. the example-question measurement, structured-data source inventory, and 3-source real-data report. |

## Legacy ELT — usage

The legacy stack is frozen but still runnable for archaeology /
reference:

```bash
docker compose up -d           # nine containers; not maintained
```

Its content history is preserved in git (commits before 2026-04-30).
