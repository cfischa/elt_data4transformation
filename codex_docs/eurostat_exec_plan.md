# Implement Eurostat metadata and dataset ingestion

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

Maintain this document in accordance with `codex_docs/PLANS.md`.

## Purpose / Big Picture

Enable the ELT platform to ingest Eurostat datasets end-to-end, matching the existing Destatis and GESIS flows. After this change the pipeline can:
- crawl Eurostat metadata into ClickHouse (`raw.eurostat_metadata`),
- expose Eurostat datasets through the canonical metadata view used by the topic classifier,
- download Eurostat statistical series for topic-selected datasets into `raw.topic_selected_payloads`,
- offer a reusable connector with tested support for metadata discovery, dataset metadata, and data extraction.

A user can verify the outcome by running the Airflow DAG (or invoking the loader functions) to populate metadata, classify Eurostat datasets, and execute the topic-selected ingest CLI to pull the actual data payload, observing persisted rows in ClickHouse and locally persisted raw files.

## Progress

- [x] (2025-10-21 23:05Z) Draft ExecPlan and gather repository context.
- [x] (2025-10-21 23:06Z) Implement EurostatConnector metadata/data endpoints with local persistence and tests.
- [x] (2025-10-21 23:09Z) Add ClickHouse loader utilities, SQL DDL, and metadata adapter/view updates for Eurostat.
- [x] (2025-10-21 23:10Z) Extend topic classifier and topic-selected ingest pipeline to recognise Eurostat sources.
- [x] (2025-10-21 23:11Z) Create Airflow DAG (and supporting script if required) to ingest Eurostat metadata into ClickHouse.
- [x] (2025-10-21 23:12Z) Run automated tests and document validation outcomes.
- [x] (2025-10-21 23:13Z) Finalise retrospective and ensure documentation/tests up to date.

## Surprises & Discoveries

- Observation: Pytest default configuration lacked an async plugin, so marked async tests failed until converted to explicit asyncio.run invocations.
  Evidence: `pytest` errors complaining about missing async support and the passing rerun after wrapping tests with asyncio.run.

## Decision Log

- Decision: Persist Eurostat extraction payloads as JSON blobs combining metadata and tabular records for downstream review.
  Rationale: Mirrors existing ingestion expectations for topic-selected payloads while keeping the structure inspectable without ClickHouse access.
  Date/Author: 2025-10-21 / assistant

## Outcomes & Retrospective

Completed connector, adapter, and ingestion integrations for Eurostat. Metadata now flows into ClickHouse via a dedicated loader and Airflow DAG, the topic classifier recognises the new source, and topic-selected ingestion can fetch datasets end-to-end. Unit tests covering the connector and adapter (`pytest -o addopts= tests/unit/test_eurostat_connector.py`, `pytest -o addopts= tests/unit/test_metadata_adapters.py`) demonstrate the behaviour without relying on external services. Remaining follow-up: extend end-to-end integration tests once ClickHouse and Airflow environments are available.

## Context and Orientation

The repository already supports Destatis and GESIS. Key references:
- `connectors/base_connector.py` provides async HTTP helper logic.
- `connectors/destatis_connector.py` and `connectors/gesis_connector.py` show patterns for metadata ingestion and persistence.
- `elt/adapters/metadata.py` normalises metadata per source and feeds `pipeline/topic_classifier.py` via `canonicalize_metadata`.
- `elt/loader_clickhouse.py` hosts helpers to create tables and upsert metadata payloads.
- `sql/create_datasets_unified_view.sql` unions source metadata into the classifier view.
- `pipeline/topic_selected_ingest.py` loads raw payloads for classifier selections.
- Airflow DAGs like `dags/fetch_destatis_metadata_clean.py` and `dags/fetch_gesis_metadata_dag.py` orchestrate metadata ingestion.

Eurostat’s REST API (statistics dissemination v1.0) exposes:
- `GET /datasets` for paginated dataset catalogue (fields: `code`, `label`, `description`, `lastUpdate`, `keywords`, `dimensions`, etc.).
- `GET /datasets/{dataset}` for detailed metadata including dimension labels, units, update timestamps.
- `GET /data/{dataset}` for data cubes, with query parameters such as `time`, `filter[geo]=DE`, etc. Responses include `dimension` metadata and a flat `value` map that we must expand to tabular records.

The connector should preserve rate limiting, surface update timestamps, and write downloaded data to disk for lineage (similar to Destatis).

## Plan of Work

1. **Connector implementation**
   - Replace stubs in `connectors/eurostat_connector.py` with real logic using `aiohttp` via `BaseConnector`.
   - Add helper methods: `_request_json`, `list_datasets`, `get_dataset_metadata`, `fetch_dataset`, `_expand_dataset_values`, `save_dataset_payload` leveraging `PersistenceManager` under `./data/raw/eurostat`.
   - Honour query parameters for language (`en` default), pagination, filters, and incremental updates via `lastUpdate`.
   - Emit `RawIngestion` records on successful/failed pulls mirroring Destatis (source `DataSource.EUROSTAT`).
   - Provide asynchronous context manager support (already via base class) and ensure `fetch_data` yields structured dicts containing metadata and record arrays.

2. **Adapters and loaders**
   - Introduce `adapt_eurostat_metadata` in `elt/adapters/metadata.py` capturing title, description, keywords (tags + themes), variables (dimension ids), latest update from Eurostat metadata, and register it.
   - Extend `ClickHouseLoader` with creation/upsert utilities for Eurostat metadata and dataset payload storage (e.g., `create_eurostat_metadata_table`, `upsert_eurostat_metadata`, `create_eurostat_dataset_table`, `insert_eurostat_dataset_payload`). Tables live under `raw` schema and align with existing ReplacingMergeTree pattern.
   - Add SQL DDL file `sql/create_eurostat_metadata_table.sql` defining schema (dataset_code, title, description, last_update DateTime64, keywords Array(String), variables Array(String), raw_metadata JSON string, fetched_at DateTime64, ingestion_time DateTime64, etc.). Provide indexes as needed.
   - Update `sql/create_datasets_unified_view.sql` to union Eurostat metadata into the canonical view, mapping to `source = 'eurostat'` and storing raw JSON.

3. **Pipeline integration**
   - Modify `pipeline/topic_classifier.py` to include Eurostat in defaults for `fetch_metadata_with_adapters`, query ClickHouse table `raw.eurostat_metadata`, and canonicalise via new adapter.
   - Update `topic_selected_ingest.py` to add `_extract_eurostat` that uses `EurostatConnector.fetch_dataset` (with optional selection payload for filters) and persists results similar to Destatis (JSON payload, record count). Register extractor in `_run_extractions`.
   - Ensure `bucket_by_source` path handles `'eurostat'` automatically (no change needed) and `dispatch_to_sources` uses new extractor.

4. **Airflow DAG & orchestration**
   - Create `dags/fetch_eurostat_metadata_dag.py` mirroring GESIS/Destatis patterns: test connectivity, fetch paginated metadata via connector, upsert into ClickHouse using new loader, validate row counts.
   - Provide CLI helper or script if necessary (e.g., in `scripts/`) for manual metadata ingestion.

5. **Testing**
   - Add unit tests with `pytest` + `aioresponses` (or manual `aiohttp` patching) under `tests/unit/test_eurostat_connector.py` verifying metadata parsing, data expansion, and incremental filtering logic without hitting network.
   - Extend `tests/test_connectors.py` to include async smoke test ensuring connector instantiates and exposes new methods.
   - Add tests for metadata adapter canonicalisation.
   - Provide fixtures for sample Eurostat API responses (store under `tests/fixtures/eurostat/`).

6. **Documentation & config**
   - Update `README.md` / relevant docs to mention Eurostat ingestion workflow if coverage gaps exist.
   - Document environment variables (`EUROSTAT_API_KEY` not required but mention) and usage instructions if needed.

## Concrete Steps

1. Implement connector logic and persistence helpers in `connectors/eurostat_connector.py`, ensuring type hints and logging consistent with repository style.
2. Add adapter + registration in `elt/adapters/metadata.py` along with helper functions to extract keywords/variables.
3. Extend `elt/loader_clickhouse.py` with Eurostat table helpers and upsert/insert routines; create new SQL DDL `sql/create_eurostat_metadata_table.sql`.
4. Update `sql/create_datasets_unified_view.sql` to include Eurostat metadata union.
5. Modify `pipeline/topic_classifier.py` and `pipeline/topic_selected_ingest.py` to integrate Eurostat flows.
6. Create Airflow DAG `dags/fetch_eurostat_metadata_dag.py` orchestrating metadata ingestion.
7. Add tests under `tests/unit/` plus update `tests/test_connectors.py`; include fixtures for mocked responses.
8. Run `pytest -o addopts= tests/unit/test_eurostat_connector.py` and `pytest -o addopts= tests/unit/test_metadata_adapters.py` (overriding coverage addopts) to validate the Eurostat connector and adapter. (Skip broader integration tests if external services are unavailable.)
9. Update documentation if required and review code for style compliance.

## Validation and Acceptance

- `pytest tests/unit -k eurostat` passes using mocked responses.
- `pytest tests/test_connectors.py::test_eurostat_connector` (new) passes, confirming instantiation and key methods.
- Manual dry-run of metadata ingestion command (e.g., `python -m dags.fetch_eurostat_metadata_dag` task function) logs successful row counts (can be simulated or mocked if ClickHouse unavailable).
- Querying ClickHouse (if available) `SELECT COUNT(*) FROM raw.eurostat_metadata` after DAG run shows non-zero rows (document expected sample output even if mocked).
- Topic-selected ingest script includes Eurostat extractions without raising `missing_extractor`.

## Idempotence and Recovery

- Metadata ingestion uses upsert semantics keyed by dataset code, so re-running only updates changed rows.
- Connector writes payload files with timestamps; ensure old files cleaned or stored deterministically if duplicates undesired.
- DAG retries handle transient HTTP issues via built-in retry/backoff in BaseConnector; describe how to resume after failure (rerun DAG, previous rows safely overwritten).

## Artifacts and Notes

- Capture sample Eurostat API responses (mock JSON) under `tests/fixtures/eurostat/` for reproducibility.
- Document ClickHouse DDL executed for new tables in commit message or docs for ops team reference.

## Interfaces and Dependencies

- `connectors.eurostat_connector.EurostatConnector` exposing:
      async def list_datasets(self, page: int = 1, page_size: int = 100, search: Optional[str] = None, since: Optional[datetime] = None) -> Dict[str, Any]
      async def get_dataset_metadata(self, dataset_id: str, language: str = "en") -> Dict[str, Any]
      async def fetch_dataset(self, dataset_id: str, filters: Optional[Dict[str, List[str]]] = None, language: str = "en", time_range: Optional[Tuple[str, str]] = None) -> Dict[str, Any]
      async def fetch_data(self, **kwargs) -> AsyncGenerator[Dict[str, Any], None]
- `elt.adapters.metadata.adapt_eurostat_metadata(row: Dict[str, Any]) -> CanonicalDatasetMetadata` registered via `register_adapter("eurostat", adapt_eurostat_metadata)`.
- `elt.loader_clickhouse.ClickHouseLoader` new methods:
      def create_eurostat_metadata_table(self) -> None
      def upsert_eurostat_metadata(self, records: List[Dict[str, Any]], batch_size: int = 1000) -> int
      def create_eurostat_dataset_table(self) -> None (if raw payload persistence needed)
      def insert_eurostat_dataset_payload(self, rows: List[Dict[str, Any]], batch_size: int = 1000) -> int
- `pipeline.topic_classifier.fetch_metadata_with_adapters` updated to support `'eurostat'` path.
- `pipeline.topic_selected_ingest._extract_eurostat` new coroutine returning `List[IngestionRecord]`.
- Airflow DAG function `fetch_eurostat_metadata_dag` referencing new loader/connector.
