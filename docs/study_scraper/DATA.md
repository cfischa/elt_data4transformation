# Data shape

Everything lives in the **`study_scraper`** Postgres schema. There are
four tables; only two carry semantic content (`studies`, `crawl_runs`)
and the others are bookkeeping.

```
study_scraper
├── studies                -- one row per discovered study
├── crawl_runs             -- one row per CLI invocation
├── crawl_run_studies      -- which run touched which study
└── schema_versions        -- applied migration numbers
```

## `study_scraper.studies` — one row per study

```
              column              |     type     | notes
----------------------------------+--------------+------------------------------------
 id                               | char(64)     | sha256(canonical_url); deterministic
 canonical_url                    | text UNIQUE  | the single source-of-truth URL
 source_urls                      | text[]       | every URL we've seen the study at
 title                            | text         |
 authors                          | text[]       | "Last, First" strings as published
 publisher                        | text         |
 publication_date                 | date         | day, month, or year-only; year-only -> Jan 1
 language                         | text         | ISO 639-1 (de, en, ...)
 topic_ids                        | text[]       | matched topics; backed by GIN index
 topic_scores                     | jsonb        | { "klima": 0.5 } - per-topic confidence
 has_quantitative_data            | bool         | cheap heuristic (n=, %, "Befragung", ...)
 abstract                         | text         | from Dublin Core / OpenAlex / etc.
 key_findings                     | text[]       | populated by extractors in later phases
 survey_metadata                  | jsonb        | { sample_size, fieldwork_*, methodology, ... }
 raw_artifact_ref                 | text         | storage URI for PDF/HTML (later phase)
 fetched_at                       | timestamptz  | when we *last* fetched (updated on upsert)
 source_id                        | text         | "ssoar" | "openalex" | "bundestag" | ...
 provenance                       | jsonb        | discovery_source, extractor_version, ...
 created_at, updated_at           | timestamptz  | row lifecycle; auto-set
```

### Indexes you can rely on

| Index                       | When you query…                              |
| --------------------------- | -------------------------------------------- |
| `studies_pkey`              | by id                                        |
| `studies_canonical_url_key` | by URL (UNIQUE — enables upsert)             |
| `studies_topic_ids_gin`     | `WHERE 'klima' = ANY(topic_ids)`             |
| `studies_source_id_idx`     | filter by `source_id`                        |
| `studies_pub_date_idx`      | recent-first ordering                        |
| `studies_fetched_at_idx`    | "what did we ingest most recently"           |

### Example row (as it sits today)

```json
{
  "id": "1f70038cf8e8ce60061ae0acb7003454cefc98f82b8720599b0f23f37d65bbb2",
  "canonical_url": "https://www.ssoar.info/ssoar/handle/document/24901",
  "source_urls":   ["https://www.ssoar.info/ssoar/handle/document/24901"],
  "title":         "Erdgas für den Klimaschutz? Chancen und Risiken einer verstärkten Gasnutzung in Europa",
  "authors":       ["Bösche, Eva"],
  "publisher":     "Stiftung Wissenschaft und Politik (SWP)",
  "publication_date": "2010-01-01",
  "language":      "de",
  "topic_ids":     ["klima"],
  "topic_scores":  { "klima": 0.5 },
  "has_quantitative_data": false,
  "abstract":      "Die Studie analysiert die Möglichkeit, durch eine verstärkte Nutzung von Erdgas in Europa rund 85 Mio. Tonnen CO2-Emissionen langfristig einzusparen…",
  "source_id":     "ssoar",
  "provenance":    { "discovery_source": "ssoar", "discovery_query": "klima", "extractor_version": "phase4-v1" },
  "fetched_at":    "2026-05-28T17:04:14Z",
  "created_at":    "2026-05-25T08:41:45Z",
  "updated_at":    "2026-05-28T17:04:14Z"
}
```

`created_at < updated_at` means the row was upserted on a later run (the
study was re-discovered). Re-runs are **idempotent**: `id` is derived
from `canonical_url`, so re-discovering a study updates the existing row
rather than inserting a duplicate.

## `study_scraper.crawl_runs` — bookkeeping per CLI invocation

One row per `python -m study_scraper run …`.

```
       column      |     type     | notes
-------------------+--------------+----------------------------
 id                | uuid         | the run id
 source_id         | text         | "ssoar" | "openalex" | ...
 topic_id          | text         | from topics.csv
 started_at        | timestamptz  |
 finished_at       | timestamptz  | NULL while still running
 candidates_seen   | int          | yielded by the source
 candidates_kept   | int          | passed the topic filter and were upserted
 errors            | int          | per-candidate exception count
 parameters        | jsonb        | { limit, min_score }
 notes             | text         |
```

## `study_scraper.crawl_run_studies` — many-to-many

`(run_id, study_id, is_new)`. `is_new` is True only on the run that
*first* inserted the study row; subsequent runs that re-discover it
record `is_new = False`.

## Common queries

### "Give me the latest top-N studies for a topic, newest first"

```sql
SELECT title, canonical_url, topic_scores->'klima' AS score, publication_date
FROM   study_scraper.studies
WHERE  'klima' = ANY(topic_ids)
ORDER  BY publication_date DESC NULLS LAST
LIMIT  20;
```

### "What's the coverage per topic?"

```sql
SELECT unnest(topic_ids) AS topic, COUNT(*) AS n
FROM   study_scraper.studies
GROUP  BY topic
ORDER  BY n DESC;
```

### "How was the last run for source X?"

```sql
SELECT *
FROM   study_scraper.crawl_runs
WHERE  source_id = 'ssoar'
ORDER  BY started_at DESC
LIMIT  1;
```

### "Which studies were first discovered by the most recent run?"

```sql
SELECT s.title, s.canonical_url
FROM   study_scraper.crawl_run_studies j
JOIN   study_scraper.crawl_runs   r ON r.id = j.run_id
JOIN   study_scraper.studies      s ON s.id = j.study_id
WHERE  r.id = (SELECT id FROM study_scraper.crawl_runs ORDER BY started_at DESC LIMIT 1)
  AND  j.is_new
ORDER  BY s.title;
```

## How to read it from the CLI

```bash
# Browse the latest 20 studies for a topic
python -m study_scraper list --topic klima

# Show the per-source / per-topic coverage and recent runs
python -m study_scraper status                # (lands with Phase 5)

# Drop into psql for ad-hoc queries
psql "$POSTGRES_URL" -c "SELECT title FROM study_scraper.studies LIMIT 5"
```

## How to read it from Python

```python
from study_scraper.config import get_settings
from study_scraper.storage import PostgresStorage, resolve_database_url

settings = get_settings()
storage  = PostgresStorage(
    resolve_database_url(
        postgres_url=settings.postgres_url,
        supabase_url=settings.supabase_url,
        supabase_service_key=settings.supabase_service_key,
    )
)
for row in storage.list_studies(topic_id="klima", limit=10):
    print(row["title"], "—", row["topic_scores"]["klima"])
```
