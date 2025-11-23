# Eurostat Connector Documentation

This document describes the Eurostat API connector for fetching statistical data from the European Commission's Eurostat service.

## Overview

The Eurostat connector provides access to the Eurostat Dissemination API (SDMX 2.1 / JSON-stat) with support for:
- Metadata discovery via Table of Contents (TOC)
- Incremental updates based on "last update" timestamps
- JSON-stat data expansion into flat records
- Automatic handling of dimensions and labels

## Authentication

The connector supports optional API key authentication for higher rate limits, but works without credentials for standard usage.

### 1. Anonymous Access (Default)
By default, the connector runs without credentials:

```python
config = EurostatConfig()
connector = EurostatConnector(config)
```

### 2. API Key (Recommended for Production)
To use an API key (if you have a registered application):

```python
config = EurostatConfig(api_key="your_api_key")
connector = EurostatConnector(config)
```

Configuration can also be loaded from the `EUROSTAT_API_KEY` environment variable.

## Data Flow

### 1. Metadata Discovery
The connector uses the bulk Table of Contents endpoint to discover available datasets:
- **Endpoint**: `catalogue/toc/txt`
- **Function**: `list_datasets()`
- **Features**:
  - Filters by search term (title/code)
  - Filters by modification date (`since` parameter)
  - Parses hierarchical themes and update timestamps

### 2. Data Extraction
Data is fetched using the JSON-stat API:
- **Endpoint**: `statistics/1.0/data/{dataset_id}`
- **Function**: `fetch_dataset()`
- **Features**:
  - Expands JSON-stat "dimension", "value", and "status" fields into flat records
  - Supports filtering by dimension values
  - Handles time ranges

## Usage Examples

### CLI Usage
(Assuming a CLI wrapper similar to Destatis exists, or via python script)

```python
import asyncio
from connectors.eurostat_connector import EurostatConnector

async def main():
    async with EurostatConnector() as connector:
        # 1. Search for datasets
        datasets = await connector.get_available_datasets(search="energy", limit=5)
        for ds in datasets:
            print(f"Found: {ds['code']} - {ds['title']}")

        # 2. Fetch a specific dataset
        data = await connector.fetch_dataset("nrg_bal_c")
        print(f"Fetched {len(data['records'])} records")

asyncio.run(main())
```

### Incremental Loading
The connector supports fetching only datasets updated since a specific time:

```python
from datetime import datetime, timedelta

since = datetime.now() - timedelta(days=1)
async for dataset in connector.get_incremental_data(since=since):
    print(f"Processing updated dataset: {dataset['dataset_id']}")
```

## Error Handling

- **Empty Responses**: Handles cases where TOC or data endpoints return no content.
- **JSON-stat Parsing**: Robust expansion logic for dimensions and values, with fallbacks for simplified responses.
- **Rate Limiting**: Respects Eurostat's usage policies (default 80 requests/period).

## Key Classes
- `EurostatConnector`: Main entry point.
- `EurostatConfig`: Configuration handling.
- `RawIngestion`: Metadata model for tracking ingestion status.


