# DESTATIS Connector Documentation

This document describes the DESTATIS GENESIS API connector for fetching statistical data from the German Federal Statistical Office.

## Overview

The DESTATIS connector provides access to the GENESIS REST API with support for:
- Token-based and username/password authentication
- Automatic chunking of large datasets
- Rate limiting and retry logic
- Multiple output formats (JSON, CSV)
- Data validation and error handling

## Authentication

The connector supports two authentication methods:

### 1. API Token (Recommended)

Use an API token for authentication by setting the `api_token` parameter:

```python
config = DestatisConfig(api_token="your_token_here")
connector = DestatisConnector(config)
```

When using a token:
- The token is sent as `username=<token>&password=` in query parameters
- No additional headers are required
- This is the preferred method for production use

### 2. Username/Password (Fallback)

For traditional username/password authentication:

```python
config = DestatisConfig(
    username="your_username",
    password="your_password"
)
connector = DestatisConnector(config)
```

When using username/password:
- Credentials are sent via Basic Authentication header
- Can also be provided as query parameters

## Mandatory Parameters

When fetching data from the API, the following parameters are mandatory:

- `name=<table-code>` - The table identifier (e.g., "12411-0001")
- `area=all` - Geographic area (automatically set unless overridden)
- `format=JSON` - Output format (automatically set unless overridden)

## API Endpoints

The connector uses the following GENESIS API endpoints:

- **Base URL**: `https://www-genesis.destatis.de/genesisWS/rest/2020`
- **Health Check**: `helloworld/logincheck` - Authentication and health check
- **Data Fetch**: `data/table` - Download table data
- **Metadata**: `metadata/table` - Get table schema and labels
- **Table List**: `catalogue/tables` - List available tables

## Token Management

### Token Rotation

API tokens typically expire after approximately 18 months. To rotate tokens:

1. **Request a new token** from DESTATIS support
2. **Update configuration** in your environment:
   - For Airflow: Update the `DESTATIS_TOKEN` Variable
   - For CLI: Set the `DESTATIS_TOKEN` environment variable
   - For code: Update the `DestatisConfig.api_token` parameter

3. **Test the new token**:
   ```bash
   python -m connectors.destatis_connector --table 12411-0001 --token $NEW_TOKEN
   ```

### Token Storage

**Production Environment (Airflow)**:
- Store tokens in Airflow Variables: `DESTATIS_TOKEN`
- Use Airflow's secret backend integration for enhanced security

**Development Environment**:
- Set environment variable: `export DESTATIS_TOKEN="your_token"`
- Use `.env` files (ensure they're in `.gitignore`)

**Never commit tokens to version control!**

## Usage Examples

### CLI Usage

```bash
# Basic table fetch
python -m connectors.destatis_connector --table 12411-0001 --token $DESTATIS_TOKEN

# With date range
python -m connectors.destatis_connector \
  --table 12411-0001 \
  --token $DESTATIS_TOKEN \
  --start-year 2020 \
  --end-year 2023

# Different format and area
python -m connectors.destatis_connector \
  --table 12411-0001 \
  --token $DESTATIS_TOKEN \
  --format csv \
  --area de
```

### Programmatic Usage

```python
import asyncio
from connectors.destatis_connector import DestatisConnector, DestatisConfig

async def fetch_data():
    config = DestatisConfig(api_token="your_token")
    
    async with DestatisConnector(config) as connector:
        # Fetch a single table
        file_path = await connector.fetch_table(
            table_id="12411-0001",
            area="all",
            start_year=2023,
            end_year=2023,
            fmt="json"
        )
        
        print(f"Data saved to: {file_path}")

# Run the async function
asyncio.run(fetch_data())
```

### Airflow DAG

- `fetch_destatis_metadata_clean.py`: weekly metadata crawl that authenticates against GENESIS REST, hydrates `raw.destatis_metadata`, and validates row counts.
- `topic_classifier_pipeline_dag.py`: triggered when metadata ingestion loads new rows; runs the classifier followed by topic-selected ingestion.
- `topic_selected_ingest_dag.py`: downstream runner (triggered by the classifier pipeline) that reads classifier selections and uses the connector to fetch requested cubes/tables on demand.
- The earlier prototype `destatis_extract_dag.py` has been retired; reuse the topic-selected flow for curated dataset pulls.

Required Airflow Variables / connections:
- `DESTATIS_API_KEY` or `DESTATIS_TOKEN` - API token (alternatively `DESTATIS_USER` / `DESTATIS_PASS`).
- Optional `TOPIC_INGEST_*` variables consumed by `topic_selected_ingest_dag.py` to scope extraction runs.

## Error Handling

The connector includes comprehensive error handling:

### Content Validation
- Ensures responses contain valid JSON
- Warns about unexpected response structure
- Raises `DestatisInvalidContent` for HTML/non-JSON responses

### Rate Limiting
- Automatically respects API rate limits (≈30 requests/minute)
- Implements exponential backoff retry logic
- Handles HTTP 429 responses with `Retry-After` headers

### Authentication Errors
- Clear error messages for invalid tokens/credentials
- Automatic retry for transient auth issues
- Fallback to Basic Auth when token auth fails

## Data Processing

### Response Validation
The connector validates API responses for expected JSON structure:
- Expected keys: `{"Identifier", "Status", "Object", ...}`
- Logs warnings for missing keys but doesn't fail the pipeline
- Validates content-type headers

### Large Dataset Handling
- Automatic chunking for tables exceeding size limits
- Time-based chunking for temporal data
- Transparent merging of chunks into final output

### Output Formats
- **JSON**: Native JSON-stat format from API
- **CSV**: Converted tabular format
- **SDMX**: Structured data exchange format

## Troubleshooting

### Common Issues

**Authentication Failures**:
```
Error: 401 Unauthorized
```
- Check token validity and expiration
- Verify token format (no extra spaces/characters)
- Test with `/helloworld/logincheck` endpoint

**Rate Limiting**:
```
Error: 429 Too Many Requests
```
- The connector automatically handles this
- Check for concurrent requests from other processes
- Consider increasing retry delays

**Content Errors**:
```
DestatisInvalidContent: HTML or unknown response
```
- API may be returning error pages instead of JSON
- Check API status and maintenance schedules
- Verify endpoint URLs and parameters

**Large Data Timeouts**:
```
httpx.TimeoutException
```
- Increase timeout in `DestatisConfig`
- Check if chunking is working properly
- Consider smaller date ranges for large tables

### Debugging

Enable debug logging:
```python
import logging
logging.getLogger('DestatisConnector').setLevel(logging.DEBUG)
```

Check API response manually:
```bash
curl -X POST "https://www-genesis.destatis.de/genesisWS/rest/2020/helloworld/logincheck" \
  -d "username=YOUR_TOKEN&password=&format=JSON"
```

## Rate Limits and Best Practices

- **Rate Limit**: Approximately 30 requests per minute
- **Concurrent Requests**: Avoid multiple simultaneous requests
- **Chunking**: Let the connector handle large datasets automatically
- **Caching**: Consider caching frequently accessed metadata
- **Monitoring**: Monitor token expiration dates
- **Scheduling**: Use off-peak hours for large extractions (early morning)

## Support and Resources

- **API Documentation**: Contact DESTATIS support for detailed API docs
- **Token Requests**: Request tokens through official DESTATIS channels
- **Technical Issues**: Check server status at DESTATIS website
- **Rate Limit Increases**: Contact DESTATIS for higher limits if needed
