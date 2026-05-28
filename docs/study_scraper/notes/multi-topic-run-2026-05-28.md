# Multi-topic, multi-source run

**Date:** 2026-05-28
**Goal of this iteration:**
1. Add OpenAlex as the second tier-1 source (Phase 5).
2. Run the scraper across **multiple topics** and **multiple sources**
   to surface real signal.
3. Stand up the control dock (status overview + topics editor).

## What ran

Three pipeline runs, fresh DB:

| # | source   | topic                   | seen | kept | errors |
|---|----------|-------------------------|-----:|-----:|-------:|
| 1 | ssoar    | klima                   |    7 |    6 |      0 |
| 2 | openalex | klima                   |    6 |    5 |      0 |
| 3 | ssoar    | migration_einwanderung  |    4 |    2 |      0 |
|   | **totals** |                       |   17 |   13 |      0 |

(Each from a real-data fixture — see
`tests/study_scraper/fixtures/{ssoar,openalex}/`. URLs and DOIs were
confirmed via WebSearch before the fixtures were built. The dev sandbox
still blocks outbound HTTP, so `--from-file` was used; the production
parsers are identical to the live path.)

## What's in the DB

```text
study scraper status @ 2026-05-28T17:11:22+00:00
================================================================
  total studies              : 13
  with quantitative data     : 8
  total crawl runs           : 3 (3 clean / 0 with errors)
  candidates seen / kept     : 17 / 13  (76.5%)

  studies per topic:
    klima                        11
    migration_einwanderung       2

  studies per source:
    ssoar                        8
    openalex                     5
```

## Two real findings the broader test surfaced

These are exactly what a broader test is for — calibration feedback
neither phase 4 nor a unit test would catch.

### 1. Migration topic vocabulary is too narrow

Only 2 of 4 migration records were kept. The two dropped ones use
**Migranten** / **Zuwanderer** (forms) where the topic only matches
**Migration** / **Zuwanderung** (stems). The rule-based matcher is
substring-on-lowercased-text, not lemmatised, so "Zuwanderer" doesn't
match "Zuwanderung".

**Fix path (operator workflow, not code):** open the topics page in the
dock, edit the `migration_einwanderung` topic to include `Migranten`,
`Zuwanderer`, `Migrant` as synonyms; the "preview matches" panel
immediately shows the count change against the studies already in the
DB. Save → next CLI run picks up the new definitions.

This is the human-in-the-loop iteration the project was set up for, and
the broader test made it visible.

### 2. Cross-source dedup gap

The same study (**"Erdgas für den Klimaschutz?"** by Bösche, 2010)
appears in both SSOAR and OpenAlex with different canonical URLs:

- SSOAR: `https://www.ssoar.info/ssoar/handle/document/24901`
- OpenAlex: `https://openalex.org/W3145167890` (DOI not present in
  OpenAlex for this record)

Because `id = sha256(canonical_url)`, these become two rows.

**Why this isn't broken by accident** — the Phase 3 TODO already listed
"deduplication beyond canonical-URL hash (DOI fallback + title
near-duplicate)" as deferred. The deferral was deliberate: defer until
we see real overlap. Now we see it.

**Fix path:** secondary-key dedup. When upserting, also look up by DOI
(if present) and by title-near-duplicate (rapidfuzz, ≥ 0.95). On a hit,
merge into the existing row instead of inserting a new one. Lands in
the next phase; tracked in `TODO.md`.

## Score distribution observations

- **OpenAlex pulls higher scores** for klima than SSOAR — abstracts are
  fuller, so more include keywords hit. Top score was an English study
  with score 0.6 (3 includes + 0 synonyms = 0.6 after the include cap).
- **Two SSOAR Energiewende records sit at 0.20** — borderline. They
  cleared because Energiewende is now an include keyword (added this
  iteration from the maintainer's "Klimawandel, Energiewende,
  Klimaschutz" test case). Without that edit they'd have been dropped.

## How to read the data

See `docs/study_scraper/DATA.md` for the full schema, example row, and
common queries. Short version:

```bash
# Coverage overview, cron-friendly
python -m study_scraper status

# Browse the latest 20 studies for a topic
python -m study_scraper list --topic klima

# Streamlit dock (operator UI)
streamlit run study_scraper/console/Home.py
```

## Reproducing this run

```bash
export POSTGRES_URL=postgresql://postgres:postgres@localhost:5544/study_scraper
python -m study_scraper migrate
python -m study_scraper run --source ssoar    --topic klima \
    --from-file tests/study_scraper/fixtures/ssoar/klima_records.xml
python -m study_scraper run --source openalex --topic klima \
    --from-file tests/study_scraper/fixtures/openalex/klima_works.json
python -m study_scraper run --source ssoar    --topic migration_einwanderung \
    --from-file tests/study_scraper/fixtures/ssoar/migration_records.xml
python -m study_scraper status
```

Live mode (when you next run from a network-enabled machine): drop the
`--from-file` flag.
