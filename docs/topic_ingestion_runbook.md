# Topic-Selected Ingestion Runbook

## Purpose
Operate the prototype workflow that converts topic classifier outputs into curated ClickHouse payloads.

## Prerequisites
- ClickHouse reachable with credentials provided via environment variables (`CLICKHOUSE_HOST`, `CLICKHOUSE_USER`, etc.).
- Metadata DAGs (`fetch_destatis_metadata_clean.py`, `fetch_gesis_metadata_dag.py`, `fetch_soep_metadata_dag.py`) now trigger `topic_classifier_pipeline_dag.py` whenever fresh metadata lands, which in turn runs the classifier and ingestion automatically.
- `sql/create_topic_selected_payloads_table.sql` applied; the CLI does this automatically on start.

## Operator Steps
1. Activate project environment and ensure connectivity.
2. Preview selections without extraction:
   ```bash
   python -m pipeline.topic_selected_ingest --dry-run --log-level DEBUG --topics migration_einwanderung
   ```
   - Confirms available datasets and grouped sources.
3. Process a limited batch (Destatis, GESIS, and SOEP extractors are available by default):
   ```bash
   python -m pipeline.topic_selected_ingest --sources destatis soep --limit 25 --since 2025-10-01T00:00:00
   ```
4. Monitor ClickHouse tables:
   - `SELECT COUNT(*) FROM raw.topic_selected_payloads;`
   - Join with `analytics.dataset_topics` to review coverage and freshness.
5. Review Streamlit dashboard (Topic Browser) for classification and ingestion coverage (new "Ingestion der Topic-Payloads" section).

## Follow-Up Tasks
1. **Connector Coverage**
   - Add extractors for additional sources (Eurostat, web scraping).
   - Enhance existing handlers with chunking, incremental fetch, and richer record counts.
2. **Error Handling & Retries**
   - Add per-source retry logic and status updates (`ingestion_status`).
   - Persist failures for inspection (e.g. new `raw.topic_selected_failures` table).
3. **Persistence Enhancements**
   - Reuse `raw.ingestions` bookkeeping with explicit `run_id`.
   - Consider partitioning payloads by topic or date for query efficiency.
4. **Automation**
   - End-to-end trigger is live via `topic_classifier_pipeline_dag.py` → update dashboards/alerts to surface pipeline status.
   - Introduce idempotent checkpoints using `metadata_hash` and watermarks.
5. **Testing**
   - Add unit tests for selection query wiring and source bucketing.
   - Implement integration tests using ClickHouse test container.
6. **Observability**
   - Surface pipeline health through existing GitHub Actions notifications or dashboards.
   - Add Grafana/alerting for ingestion latency and failure counts.
7. **Documentation**
   - Update README and architecture diagrams once the end-to-end flow is fully implemented.

This runbook provides operators and developers with current operating instructions and highlights the remaining work to deliver a production-ready ingestion loop.
