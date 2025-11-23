# New Data Sources Implementation Details

This document details the implementation of the **Destatis**, **Eurostat**, and **SOEP** data connectors, focusing on metadata extraction, selection, and data loading mechanisms.

## 1. Destatis (German Federal Statistical Office)

**Source File**: `connectors/destatis_connector.py`

### Metadata Extraction
- **Primary Method**: Uses the `catalogue/cubes` endpoint (and `catalogue/tables` as fallback) to discover available statistical objects.
- **Implementation**: `get_available_cubes` fetches a list of `CubeInfo` objects containing code, content, time coverage, and last update timestamp.
- **Legacy Support**: Fallback to `catalogue/tables` if the cubes endpoint fails.

### Metadata Selection
- **Filtering**:
  - **Search Pattern**: Supports a wildcard selection string (default `*`) passed to the API.
  - **Time-based**: `get_incremental_data` filters objects based on the `latest_update` timestamp compared to a provided `since` datetime.
- **Prioritization**: Results are sorted by `latest_update` (newest first) to prioritize fresh data.

### Data Loading
- **Endpoints**:
  - `data/cubefile`: For fetching raw cube data (preferred).
  - `data/tablefile`: For fetching table data.
- **Process**:
  1. **Chunking**: `_calculate_chunks` splits large requests based on time ranges or size estimates (max cells per request).
  2. **Extraction**: `fetch_cube` or `fetch_table` executes the request.
  3. **Format**: Supports JSON (JSON-stat), CSV, and SDMX.
  4. **Persistence**: Saves raw files to disk using `PersistenceManager` and logs `RawIngestion` records.
- **Handling**: Automatic handling of gzip compression and retries for rate limits.

---

## 2. Eurostat (European Commission)

**Source File**: `connectors/eurostat_connector.py`

### Metadata Extraction
- **Method**: Bulk retrieval of the Table of Contents (TOC).
- **Endpoint**: `catalogue/toc/txt` (returns a tab-separated file).
- **Implementation**: `list_datasets` fetches and `_parse_catalogue_toc` parses the TOC to extract dataset codes, titles, hierarchy (themes), and update dates.

### Metadata Selection
- **Filtering**:
  - **Search**: In-memory filtering of the TOC by title/code matching.
  - **Incremental**: Filters datasets where `last update of data` is newer than the `since` parameter.
  - **Language**: Selects metadata in the specified language (default "en").

### Data Loading
- **Endpoint**: `statistics/1.0/data/{dataset_id}` (JSON-stat API).
- **Process**:
  1. **Fetching**: `fetch_dataset` retrieves the JSON-stat payload.
  2. **Expansion**: `_expand_dataset_values` converts the hierarchical JSON-stat format (dimensions, indices, values) into a flat list of records (dictionaries).
  3. **Persistence**: Saves the raw JSON payload and returns the expanded records.
- **Features**:
  - Resolves dimension labels from metadata.
  - handles "status" and "flags" alongside values.

---

## 3. SOEP (Socio-Economic Panel)

**Source File**: `connectors/soep_connector.py`

### Metadata Extraction
- **Method**: Sequential fetching of indicator lists and details from the SOEP Monitor API.
- **Endpoint**: `/indicators` (list) and `/indicators/{slug}` (detail).
- **Implementation**: `fetch_metadata` iterates through pages of indicators and then fetches full details (dimensions, topics, units) for each, merging the results into a rich metadata object.

### Metadata Selection
- **Filtering**:
  - **Dataset ID**: Selection is primarily by `indicator_slug`.
  - **Incremental**: `get_incremental_data` post-filters observations where the `year` field is greater than or equal to the provided `since` year.
- **Constraint**: Does not support server-side filtering by modification date; client-side filtering is applied to observations.

### Data Loading
- **Endpoint**: `/indicators/{slug}/observations`.
- **Process**:
  1. **Streaming**: `stream_observations` handles pagination to retrieve all time-series data points for a given indicator.
  2. **Normalization**: `fetch_data` wraps the observations in a standardized payload with source and timestamp metadata.
- **Features**:
  - Pure async streaming implementation.
  - Robust error handling for individual page failures.
