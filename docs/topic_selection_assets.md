# Topic-Driven Selection Assets

This note consolidates the existing components that feed the post-classification dataset ingestion workflow.

## ClickHouse Tables & Views
- `analytics.dataset_topics`: high confidence topic assignments with scores and metadata hashes (`sql/create_dataset_topics_table.sql`).
- `analytics.dataset_topics_review`: low confidence candidates for manual triage (`sql/create_dataset_topics_review_table.sql`).
- `analytics.dataset_topics_excluded`: excluded datasets keyed by topic and exclusion terms (`sql/create_dataset_topics_excluded_table.sql`).
- `analytics.topic_classifier_runs`: execution metrics for the rule-based classifier (`sql/create_topic_classifier_runs_table.sql`).
- `analytics.datasets_unified`: canonical metadata view sourcing `raw.destatis_metadata`, `raw.gesis_metadata`, and `raw.soep_metadata` (`sql/create_datasets_unified_view.sql`).
- `raw.topic_selected_payloads`: proposed storage for topic-aligned raw dataset payloads (`sql/create_topic_selected_payloads_table.sql`).

Helper SQL artifacts:
- `sql/select_topic_dataset_catalog.sql`: joins latest topic assignments with canonical metadata for downstream extraction.

## Classification Code Paths
- `pipeline/topic_classifier.py`:
  - Loads taxonomy from `config/topics/taxonomy.yml`.
  - Pulls canonical metadata via `load_canonical_metadata` (prefers `analytics.datasets_unified`).
  - Applies rule-based scoring thresholds (`HIGH_SCORE_THRESHOLD=0.5`, `LOW_SCORE_THRESHOLD=0.3`).
  - Persists results to ClickHouse through `ClickHouseLoader`.
- `config/topics/topic_sources.yml`: curated hints (`dataset_id_pattern`, notes) for mapping topics to concrete sources.
- `docs/topic_classifier_storage.md`: design rationale, retention strategy, and operational notes for classifier outputs.

## Connector & Loader Building Blocks
- `connectors/destatis_connector.py`: async GENESIS cube discovery/extraction supporting incremental pulls.
- `connectors/gesis_connector.py`: SPARQL-based metadata fetcher for research datasets.
- `connectors/soep_connector.py`: SOEP Monitor API client streaming indicator metadata and observations.
- `elt/loader_clickhouse.py`: handles ClickHouse connections, schema bootstrap, and batch inserts (`insert_json_data`, `insert_dataframe`).
- `elt/adapters/metadata.py`: canonicalises heterogeneous source metadata and computes `metadata_hash`.

## Orchestration & Monitoring
- Airflow DAGs (`dags/`):
  - `fetch_destatis_metadata_clean.py`: weekly metadata ingestion into `raw.destatis_metadata`.
  - `fetch_gesis_metadata_dag.py`: daily SPARQL crawl into `raw.gesis_metadata`.
  - `fetch_soep_metadata_dag.py`: daily SOEP Monitor metadata ingestion into `raw.soep_metadata`.
  - `dawum_ingest_dag.py`: daily polling data load into `raw.dawum_polls`.
  - `topic_classifier_pipeline_dag.py`: triggered whenever metadata DAGs ingest new records; runs classifier then ingestion.
  - `topic_selected_ingest_dag.py`: drives post-classification extraction using `pipeline/topic_selected_ingest.py` (triggered by the pipeline DAG).
  - `dbt_transform_dag.py`: orchestrates ClickHouse transformations and tests.
- Legacy scaffolds for cube extraction and generic loaders have been removed; favor topic-driven ingestion and dbt orchestration going forward.
- Metadata DAGs short-circuit when no fresh rows are ingested, ensuring the classifier pipeline only runs when new content arrives.
- Streamlit Insights (`streamlit_app/pages/3_Topic_Browser.py`):
  - Surfaces coverage of `dataset_topics`, `dataset_topics_review`, and `dataset_topics_excluded`.
  - Includes user messaging pointing to `python -m pipeline.topic_classifier`.

These assets provide the source-of-truth for selecting topic-aligned datasets and the primitives required to pull and stage the corresponding raw data into ClickHouse.
