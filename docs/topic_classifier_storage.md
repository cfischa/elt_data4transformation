# Topic Classifier Storage Plan

This note captures how to persist classification results and topic-aligned data products in ClickHouse while supporting heterogeneous metadata schemas coming from DESTATIS, GESIS, and future sources.

## Goals
- Track classifier decisions with confidence, rationale, and provenance (source/metadata hash/model version).
- Query topics across sources with a unified catalog despite differing metadata fields.
- Maintain history for reclassification (ReplacingMergeTree) and support incremental updates keyed on each source's `latest_update` semantics.
- Provide staging tables that downstream dbt models and Streamlit dashboards can query.

## Proposed Schema

### 1. `analytics.dataset_topics`
Stores the classifier output per dataset/resource.

| Column              | Type                  | Notes                                                             |
|---------------------|-----------------------|-------------------------------------------------------------------|
| `dataset_id`        | String                | Natural key from source (e.g., Destatis table code, GESIS URI).   |
| `source`            | LowCardinality(String)| `destatis`, `gesis`, `eurostat`, …                               |
| `topic_id`          | LowCardinality(String)| Matching `config/topics/taxonomy.yml` ids.                        |
| `score`             | Float32               | Final confidence (0.0–1.0).                                       |
| `classifier_version`| String                | Semantic version of the rules/LLM combo.                          |
| `matched_terms`     | String                | JSON string capturing keyword hits, embedding neighbors.          |
| `rationale`         | String                | Brief explanation from classifier (LLM output).                   |
| `metadata_hash`     | FixedString(32)       | Hash of canonicalised metadata to enable dedupe/caching.          |
| `decided_at`        | DateTime64(3)         | When the classification ran.                                      |
| `event_date`        | Date MATERIALIZED     | `toDate(decided_at)` for partitioning.                            |

Engine: `ReplacingMergeTree(decided_at)`  
Partitioning: `toYYYYMM(event_date)`  
Order by: `(source, dataset_id, topic_id, decided_at)`

### 2. `analytics.dataset_topics_review`
Holds low-confidence candidates that fell below the high-confidence threshold but still matched topic hints (`sql/create_dataset_topics_review_table.sql`).

| Column           | Type                  | Notes                                        |
|------------------|-----------------------|----------------------------------------------|
| `dataset_id`     | String                | Natural key from source.                     |
| `source`         | LowCardinality(String)| Metadata source (destatis, gesis, …).        |
| `candidate_topic`| LowCardinality(String)| Topic with the highest sub-threshold score.  |
| `score`          | Float32               | Best score below `HIGH_SCORE_THRESHOLD`.     |
| `matched_terms`  | String                | JSON array of matched keywords/dataset hints.|
| `rationale`      | String                | Explanation for the match.                   |
| `metadata_hash`  | FixedString(32)       | Canonical metadata hash.                     |
| `decided_at`     | DateTime64(3)         | Timestamp of classification.                 |

Engine: `ReplacingMergeTree(decided_at)`  
Partitioning: `toYYYYMM(event_date)`  
Order by: `(source, dataset_id, candidate_topic, decided_at)`

### 3. `analytics.dataset_topics_excluded`
Captures datasets flagged as “nicht relevant” based on strong exclusion keywords (`sql/create_dataset_topics_excluded_table.sql`).

| Column        | Type                  | Notes                                        |
|---------------|-----------------------|----------------------------------------------|
| `dataset_id`  | String                | Natural key from source.                     |
| `source`      | LowCardinality(String)| Metadata source.                             |
| `topic_id`    | LowCardinality(String)| Topic whose Ausschlussbegriffe fired.        |
| `exclude_terms`| String               | JSON array with matching exclusion terms.    |
| `metadata_hash`| FixedString(32)      | Canonical metadata hash.                     |
| `decided_at`  | DateTime64(3)         | Timestamp of classification.                 |

Engine: `ReplacingMergeTree(decided_at)`  
Partitioning: `toYYYYMM(event_date)`  
Order by: `(source, dataset_id, topic_id, decided_at)`

### 2. `analytics.topic_classifier_runs`
Aggregated metrics per execution of the classifier (see `sql/create_topic_classifier_runs_table.sql`).

| Column              | Type        | Notes                                                  |
|---------------------|-------------|--------------------------------------------------------|
| `run_id`            | UUID        | Traceable identifier per execution.                    |
| `started_at`        | DateTime64  | UTC timestamp when run began.                          |
| `completed_at`      | DateTime64  | UTC timestamp when run finished.                       |
| `datasets_scanned`  | UInt64      | Number of metadata rows considered.                    |
| `datasets_classified`| UInt64     | Rows with at least one topic assignment.               |
| `topics_assigned`   | UInt64      | Total topic records written.                           |
| `taxonomy_version`  | String      | Version pulled from `config/topics/taxonomy.yml`.      |
| `classifier_version`| String      | Script semantic version.                               |
| `status`            | String      | `success` or `failed`.                                 |
| `error_message`     | String      | Non-empty if run failed.                               |

### 4. `analytics.datasets_unified`
A view (`sql/create_datasets_unified_view.sql`) that normalises metadata across sources:

- DESTATIS rows expose `cube_code`, `content`, `state`, `time_coverage`, etc.  
- GESIS rows contribute `title`, `description`, `issued`, `variables`, `raw_data`.

The view produces shared columns (`source`, `dataset_id`, `title`, `description`, `latest_update`, `keywords`, `variables`, `raw_metadata`) used by the classifier and other downstream consumers.

### 5. `staging.topic_indicator_facts`
Holds topic-aligned numeric series after connectors/adapters normalise distinct structures.

| Column          | Type         | Notes                                      |
|-----------------|--------------|--------------------------------------------|
| `topic_id`      | String       | Topic taxonomy id.                         |
| `indicator_id`  | String       | Stable indicator code (source + measure).  |
| `indicator_name`| String       | Human-readable name.                       |
| `source`        | String       | Data origin.                               |
| `dataset_id`    | String       | Link back to metadata row.                 |
| `geo_code`      | String       | Normalised geographic key.                 |
| `time`          | DateTime     | Observation time.                          |
| `value`         | Float64      | Numeric value.                             |
| `unit`          | String       | Unit-of-measure.                           |
| `frequency`     | String       | `A`, `Q`, `M`, etc.                        |
| `metadata`      | String       | JSON with source-specific details.         |
| `ingested_at`   | DateTime64   | Load timestamp.                            |

Engine: `ReplacingMergeTree(ingested_at)`  
Partition: `toYYYYMM(time)`  
Order by: `(topic_id, indicator_id, geo_code, time)`

## Handling Heterogeneous Metadata
- `elt/adapters/metadata.py` maps DESTATIS and GESIS rows into a canonical `CanonicalDatasetMetadata` structure with stable hashing.
- The classifier prefers reading from `analytics.datasets_unified` but falls back to adapters if the view is unavailable.
- Language-specific synonyms live in `config/topics/taxonomy.yml`, removing the need to depend on raw column names.
- `metadata_hash` equals the MD5 of the canonical payload and acts as a cache key to skip unchanged records.
- Datasets are segmented into three buckets: confident assignments (`analytics.dataset_topics`), borderline matches awaiting review (`analytics.dataset_topics_review`), and explicit exclusions (`analytics.dataset_topics_excluded`). Remaining datasets appear as unlabeled in the Streamlit browser.
- Confidence thresholds: scores ≥ 0.5 land in `dataset_topics`; scores between 0.3 and 0.5 fall into the review bucket; exclude keywords can override and push a dataset into the exclusion table.

## Incremental Maintenance
The CLI `python -m pipeline.topic_classifier` performs the following steps:
1. Load taxonomy terms and build rule sets.
2. Ensure DDL for tables/views is applied via `sql/*topic*.sql`.
3. Query `analytics.datasets_unified` (or fall back to adapters) optionally filtered by `--since` or `--sources`.
4. Generate keyword-based topic matches, recording matched terms and scores.
5. Upsert confident matches into `analytics.dataset_topics`. Borderline matches flow into `analytics.dataset_topics_review`, and exclusion hits populate `analytics.dataset_topics_excluded`. Run metrics land in `analytics.topic_classifier_runs`.

Checkpointing can be achieved by invoking the CLI with `--since` equal to the latest `completed_at` stored in the metrics table.

## Backfill & Re-training
- Full re-run is achievable by truncating `analytics.dataset_topics` (after backup) and replaying metadata.
- Store classifier assets (prompt templates, taxonomy versions) under `config/topics/` for reproducibility.

## Operational Notes
- Trigger a full run with `python -m pipeline.topic_classifier --sources destatis --log-level INFO`.\
  Die regelbasierte Klassifikation verarbeitet derzeit ~11.500 DESTATIS-Datensätze in wenigen Sekunden, daher ist kein Limit nötig.
- Nutze `--dry-run`, um neue Regeln zu testen, ohne Ergebnisse zu persistieren.
- dbt-Modell `models/staging/stg_topic_indicator_facts.sql` stellt Topic-Facts für Analytics bereit.
- `analytics.topic_classifier_runs` liefert Monitoring-Daten (Durchsatz, Status, Errors).
- Der Streamlit-Report **Topic Classification Browser** (`streamlit_app/pages/3_Topic_Browser.py`) zeigt Summaries, Detailansicht sowie separate Tabellen für Prüf- und Ausschlussfälle.
- Plane Erweiterungen (Embeddings, LLM-Layer) in `pipeline/topic_classifier.py` und erhöhe `CLASSIFIER_VERSION`, sobald ein neues Verfahren live geht.

## TODO
- TODO: Klassifikationslauf automatisieren (z.B. Airflow DAG/Scheduler), sodass neue Metadaten ohne manuelles Anstoßen verarbeitet werden.
- TODO: Automatisiertes Monitoring (Dashboards/Alerts) für die Prüf- und Ausschlusslisten ergänzen.
- TODO: OpenAI-API für diese Low-Confidence-Fälle einbinden; Hook im Anschluss an den Rule-basierten Schritt in `pipeline/topic_classifier.py`.
