# Architecture Documentation

## System Overview

The `bnb_data4transformation` project is a data platform designed to aggregate, process, and visualize political and statistical data from various sources (DAWUM, Destatis, Eurostat, GESIS, SOEP). It follows an ELT (Extract, Load, Transform) architecture, leveraging Airflow for orchestration, ClickHouse as the high-performance analytical database, dbt for data transformation, and Streamlit for the user interface.

## High-Level Architecture

```mermaid
graph TD
    subgraph Sources [External Data Sources]
        DAWUM[DAWUM API]
        DESTATIS[Destatis Genesis API]
        EUROSTAT[Eurostat API]
        GESIS[GESIS Repository]
        SOEP[SOEP Metadata]
    end

    subgraph Orchestration [Orchestration & Ingestion]
        AF[Airflow Scheduler/Worker]
        CONN[Connectors (Python)]
        
        AF -->|Triggers| CONN
        AF -->|Triggers| DBT
        AF -->|Triggers| CLS[Topic Classifier]
    end

    subgraph Storage [Data Warehouse (ClickHouse)]
        Raw[Raw Layer\n(raw.*)]
        Staging[Staging Layer\n(staging.*)]
        Analytics[Analytics Layer\n(analytics.*)]
    end

    subgraph Processing [Transformation & Classification]
        DBT[dbt (Data Build Tool)]
        CLS
        
        CONN -->|Upsert/Insert| Raw
        DBT -->|Reads| Raw
        DBT -->|Materializes| Staging
        DBT -->|Materializes| Analytics
        
        CLS -->|Reads Metadata| Analytics
        CLS -->|Writes Topics| Analytics
    end

    subgraph Presentation [Presentation Layer]
        SL[Streamlit App]
        NB[Jupyter Notebooks]
    end

    Sources -->|Data Fetch| CONN
    
    SL -->|Query| Analytics
    NB -->|Query| Analytics

    style Storage fill:#f9f,stroke:#333,stroke-width:2px
    style Orchestration fill:#ccf,stroke:#333,stroke-width:2px
    style Presentation fill:#cfc,stroke:#333,stroke-width:2px
```

## Module Boundaries & Responsibilities

### 1. Ingestion Layer (`connectors/`, `dags/`)
-   **Responsibility**: Fetch data from external APIs and load it "as-is" into the database.
-   **Components**:
    -   **Connectors**: Source-specific Python classes (e.g., `DawumConnector`, `DestatisConnector`) handling API authentication, rate limiting, and response parsing.
    -   **Airflow DAGs**: Schedule ingestion intervals and manage retries.
    -   **Loader**: `elt/loader_clickhouse.py` handles efficient insertion/upsertion into ClickHouse `raw` schema.

### 2. Storage Layer (ClickHouse)
-   **Responsibility**: Store all data stages from raw ingestion to final analytical views.
-   **Schemas**:
    -   `raw`: Landing zone for ingested data (JSON strings or direct API mappings).
    -   `staging`: Cleaned and normalized data (managed by dbt).
    -   `analytics`: Business logic, aggregated views, and classified datasets (managed by dbt and classifiers).

### 3. Processing Layer (`dbt_project/`, `pipeline/`)
-   **Responsibility**: Transform raw data into usable insights and enrich metadata.
-   **Components**:
    -   **dbt**: SQL-based transformations defines the path from `raw` to `analytics`. Handles testing and documentation of models.
    -   **Topic Classifier**: Python-based pipeline (`pipeline/topic_classifier.py`) that reads unified metadata and assigns topic tags based on a taxonomy (`config/topics/taxonomy.yml`).

### 4. Presentation Layer (`streamlit_app/`)
-   **Responsibility**: visualize data for end-users.
-   **Components**:
    -   **Streamlit**: Python-based web application querying ClickHouse directly to generate charts, tables, and search interfaces.

## Data Flow Patterns

1.  **Standard Ingestion Flow**:
    `External API` -> `Connector` -> `ClickHouse (raw)` -> `dbt` -> `ClickHouse (analytics)` -> `Streamlit`

2.  **Metadata & Classification Flow**:
    `External APIs` -> `ClickHouse (raw metadata)` -> `dbt (datasets_unified view)` -> `Topic Classifier` -> `ClickHouse (dataset_topics)` -> `Streamlit`

## System Interfaces

-   **ClickHouse Interface**: Primary data interface. All components (Ingest, dbt, Streamlit) talk to ClickHouse via TCP (port 9000) or HTTP (port 8123).
-   **Airflow UI**: Management interface for pipelines (port 8081).
-   **Streamlit UI**: End-user interface (port 8501).

## Component Dependencies

-   **Airflow** depends on **Redis** (messaging) and **Postgres** (metadata).
-   **dbt** runs as a containerized task within the Airflow environment or separate container, depending on **ClickHouse**.
-   **Streamlit** depends on **ClickHouse** for data serving.
-   **Connectors** depend on external internet access and valid API credentials.

