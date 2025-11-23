# SOEP Connector Documentation

This document describes the SOEP API connector for fetching socio-economic panel data from the SOEP Monitor.

## Overview

The SOEP connector provides asynchronous access to the SOEP Monitor public API (`https://monitor.soep.de/api/v1`), exposing:
- **Indicator Metadata**: Detailed descriptions, topics, dimensions, and units.
- **Time-Series Observations**: Historical data points for specific indicators.

## Data Flow

### 1. Metadata Discovery
The connector discovers available indicators via the `/indicators` endpoint.

- **Function**: `fetch_metadata()`
- **Process**:
  1. Fetches paginated lists of indicator summaries.
  2. Enriches each summary by fetching detailed metadata (`/indicators/{slug}`).
  3. Normalizes topics, dimensions, and units into a consistent format.

### 2. Metadata Selection
Selection is primarily done by **slug** (dataset ID).

- **Filtering**:
  - **Incremental**: `get_incremental_data(since=...)` filters observations where the `year` is greater than or equal to the provided date's year.
  - **Explicit**: Users can request specific datasets by providing a list of slugs.

### 3. Data Loading
Data is streamed from the `/indicators/{slug}/observations` endpoint.

- **Function**: `fetch_data()` / `stream_observations()`
- **Process**:
  1. Iterates through requested indicator slugs.
  2. Streams paginated observations for each indicator.
  3. Yields a standardized payload containing the source, dataset slug, timestamp, and list of observations.
- **Output Structure**:
  ```json
  {
    "source": "soep",
    "dataset": "indicator_slug",
    "timestamp": "2023-10-27T10:00:00+00:00",
    "observations": [
      {"year": 2020, "value": 123.4, ...},
      ...
    ],
    "filters": {...}
  }
  ```

## Usage Examples

### Basic Usage
```python
from connectors.soep_connector import SOEPConnector

async with SOEPConnector() as connector:
    # Fetch metadata for all indicators (first page only for demo)
    metadata = await connector.fetch_metadata(max_pages=1)
    
    # Fetch data for a specific indicator
    async for result in connector.fetch_data(datasets=["pglabgro"]):
        print(f"Fetched {len(result['observations'])} observations for {result['dataset']}")
```

### Incremental Loading
Fetch only data from recent years:

```python
from datetime import datetime

since = datetime(2020, 1, 1)
async for result in connector.get_incremental_data(since=since, datasets=["pglabgro"]):
    print(f"New observations since 2020: {len(result['observations'])}")
```

## Configuration

The connector is configured via `SOEPConfig`:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `base_url` | `https://monitor.soep.de/api/v1` | API endpoint base URL |
| `rate_limit_requests` | 60 | Max requests per period |
| `rate_limit_period` | 60 | Time period in seconds |
| `per_page` | 100 | Items per page (max 500) |

## Error Handling
- **Pagination**: Automatically handles pagination logic and stops on empty or invalid responses.
- **Rate Limiting**: Respects configured rate limits to avoid API blocking.
- **Defensive Parsing**: Logs warnings and continues if specific fields or details are missing or malformed, ensuring partial failures don't crash the pipeline.

