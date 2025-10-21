# Topic-Selected Dataset Ingestion Flow

## Overview
The ingestion flow turns classifier outputs (`analytics.dataset_topics`) into concrete data loads that persist curated datasets in ClickHouse for downstream analytics.

```
Classifier -> Selection Query -> Extraction Runner -> ClickHouse Loader -> Monitoring
```

## 1. Selection Stage
1. Execute `sql/select_topic_dataset_catalog.sql` to obtain the latest `(source, dataset_id, topic_id)` triplets with supporting metadata.
2. Optionally filter by:
   - Topic IDs (e.g. via `config/topics/topic_sources.yml` groupings).
   - `latest_update` > watermark for incremental pulls.
   - Minimum score threshold above `HIGH_SCORE_THRESHOLD`.
3. Register selections per source to simplify routing to connectors.

## 2. Task Chunking
- Group the selected rows by `source`.
- For each source, chunk the list based on connector capabilities (e.g. Destatis rate limits, batch size hints from metadata).
- Persist chunk definitions in memory or temp storage for idempotent retries.

## 3. Extraction Execution
- For each chunk:
  1. Instantiate the appropriate connector:
     - `DestatisConnector` for GENESIS cubes (implemented: downloads raw cube files, stringifies their contents, and removes temp artefacts after persistence).
     - `GESISConnector` for SPARQL datasets (implemented: executes CONSTRUCT queries and stores prettified JSON metadata).
     - Future adapters for Eurostat, SOEP, web scraping, etc.
  2. Fetch raw payloads (JSON, CSV, RDF, etc.) and normalise into a uniform structure:
     ```json
     {
       "source": "...",
       "dataset_id": "...",
       "topic_id": "...",
       "payload_format": "json|csv|rdf",
       "payload": "<stringified content>",
       "records_count": <int>,
       "metadata_hash": "...",
       "decided_at": "ISO timestamp"
     }
     ```
  3. Capture operational metadata:
     - `run_id` (UUID shared across the batch run).
     - `ingestion_started_at` / `ingestion_completed_at`.
     - `ingestion_status` (success, failed, partial).

## 4. Persistence
- Use `ClickHouseLoader` to ensure `raw.topic_selected_payloads` exists.
- Insert payload records via `insert_json_data` or `insert_dataframe`.
- Capture ingestion metadata in parallel (e.g. reuse `raw.ingestions` for auditing).
- Optionally materialise parsed facts into topic-specific tables (future dbt models).

## 5. Orchestration Options
- **CLI Runner** (`python -m pipeline.topic_selected_ingest`):
  - Accepts arguments `--topics`, `--sources`, `--since`, `--limit`, `--min-score`, `--dry-run`.
  - Handles schema bootstrap, selection query execution, source dispatch (Destatis + GESIS), and persistence to `raw.topic_selected_payloads`.
  - Dry-run mode lists selections without triggering connectors.
- **Airflow DAG** (`dags/topic_selected_ingest_dag.py`):
  - Runs daily by default (schedule configurable through Airflow Variables).
  - Populates CLI options from Variables (`TOPIC_INGEST_TOPICS`, `TOPIC_INGEST_SOURCES`, `TOPIC_INGEST_SINCE`, `TOPIC_INGEST_LIMIT`).
  - Raises `AirflowFailException` when the CLI exits non-zero so operators see failed runs immediately.

## 6. Monitoring & Backpressure
- Streamlit Topic Browser now includes an ingestion coverage section aggregating `raw.topic_selected_payloads` (success, failed, pending, last ingestion).
- Surface lag metrics (`now() - ingestion_completed_at`).
- On failure, persist error context within `ingestion_status` and requeue.

## 7. Incremental Strategy
- Track the maximum `decided_at` processed for each source/topic (e.g. table or Airflow Variable).
- Use `--since` guard to avoid reprocessing unchanged datasets unless `metadata_hash` changes.
- When taxonomy or classifier version increments, allow full refresh by deleting/overwriting relevant payload rows (ReplacingMergeTree).

This flow provides a repeatable mechanism to transform topic classification results into curated ClickHouse datasets ready for analytical workloads.
