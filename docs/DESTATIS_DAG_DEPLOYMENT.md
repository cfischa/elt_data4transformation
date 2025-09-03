# Destatis Metadata DAG - Production Deployment Guide

## Overview

The `fetch_destatis_metadata` DAG is now production-ready! This guide covers the final deployment steps and monitoring recommendations.

## ✅ Validation Results

All production readiness checks have passed:

- **Infrastructure**: ClickHouse connected and `destatis_metadata` table exists
- **API Connectivity**: Destatis GENESIS-Online API accessible and responsive
- **DAG Syntax**: All DAG files have valid Python syntax
- **Environment**: Credentials configured properly
- **Sample Pipeline**: End-to-end test successful (API → ClickHouse)

## 📁 Available DAG Files

Three DAG versions are available:

1. **`fetch_destatis_metadata_dag.py`** - Original version with detailed logging
2. **`fetch_destatis_metadata_dag_v2.py`** - Enhanced version with improved error handling
3. **`fetch_destatis_metadata_clean.py`** - Clean production version (RECOMMENDED)

## 🚀 Deployment Steps

### Step 1: Copy DAG to Airflow

The DAG file is already in the correct location (`dags/` folder) and will be automatically detected by Airflow.

### Step 2: Access Airflow UI

1. Open http://localhost:8081
2. Login with: `airflow` / `airflow`
3. Look for the DAG: `fetch_destatis_metadata_production`

### Step 3: Configure Credentials (Optional)

For production use with large datasets, set up Airflow Variables:

```bash
# Option 1: API Token (recommended)
airflow variables set DESTATIS_API_KEY "your_api_token_here"

# Option 2: Username/Password
airflow variables set DESTATIS_USER "your_username"
airflow variables set DESTATIS_PASS "your_password"
```

**Note**: The DAG will work with anonymous access for development/testing, but authenticated access provides:
- Higher rate limits
- Access to larger datasets
- Better reliability

### Step 4: Enable and Test the DAG

1. In Airflow UI, toggle the DAG to "ON"
2. Click "Trigger DAG" to run manually
3. Monitor the execution in "Graph View" or "Logs"

## 📊 Expected Behavior

### Normal Execution Flow

1. **Test API Connection** (30 seconds)
   - Tests connectivity with small sample
   - Validates credentials
   - Logs credential type being used

2. **Fetch All Metadata** (2-5 minutes)
   - Retrieves ALL available cube metadata (~50,000 cubes)
   - Processes and formats data for ClickHouse
   - Inserts data in batches of 1,000

3. **Validate Data** (30 seconds)
   - Verifies data was inserted correctly
   - Counts total and new records
   - Shows sample records in logs

### Performance Metrics

- **Total cubes**: ~50,000 metadata records
- **Runtime**: 3-6 minutes total
- **Data size**: ~50MB of metadata
- **Update frequency**: Weekly (configurable)

## 🔍 Monitoring and Troubleshooting

### Key Metrics to Monitor

1. **Task Success Rate**: Should be >95%
2. **Runtime**: Should complete within 10 minutes
3. **Data Freshness**: New records should appear weekly
4. **Error Patterns**: Watch for API rate limiting or timeout errors

### Common Issues and Solutions

**Issue**: "No credentials found - using anonymous access"
- **Solution**: Set up Airflow Variables or environment variables
- **Impact**: Limited to smaller datasets, may hit rate limits

**Issue**: "Table 'destatis_metadata' does not exist"
- **Solution**: Run `python scripts/safe_setup_destatis_table.py`
- **Impact**: DAG will fail until table is created

**Issue**: HTTP 429 (Rate Limited)
- **Solution**: Increase retry delays or use authenticated access
- **Impact**: Temporary delays, will retry automatically

**Issue**: ClickHouse connection errors
- **Solution**: Ensure Docker containers are running (`make up`)
- **Impact**: DAG will fail until database is accessible

### Checking Data Quality

```sql
-- View latest metadata in ClickHouse
SELECT COUNT(*) as total_cubes 
FROM raw.destatis_metadata;

-- Check today's data
SELECT COUNT(*) as todays_records 
FROM raw.destatis_metadata 
WHERE toDate(fetched_at) = today();

-- Sample recent records
SELECT cube_code, content, fetched_at 
FROM raw.destatis_metadata 
ORDER BY fetched_at DESC 
LIMIT 5;
```

## 🔧 Configuration Options

### DAG Parameters

- **Schedule**: `@weekly` (every Sunday at midnight)
- **Retries**: 3 attempts with 5-minute delays
- **Timeout**: No timeout (cubes fetch can take several minutes)
- **Max Active Runs**: 1 (prevents overlapping executions)

### Customization Options

To modify the DAG behavior, edit these parameters in the DAG file:

```python
# Change schedule
schedule="@daily"  # Run daily instead of weekly

# Change retry behavior
"retries": 5,
"retry_delay": timedelta(minutes=15),

# Change cube selection
pagelength=10000,  # Fetch fewer cubes for testing
```

## 📈 Integration with Existing Pipeline

The DAG integrates seamlessly with your existing infrastructure:

- **Storage**: Uses existing `raw.destatis_metadata` table
- **Credentials**: Shares environment variables with other connectors
- **Monitoring**: Uses existing ClickHouse and Airflow infrastructure
- **Logging**: Follows established logging patterns

## 🎯 Success Criteria

The DAG is working correctly when:

1. ✅ Executes without errors weekly
2. ✅ Inserts 40,000+ metadata records per run
3. ✅ Completes within 10 minutes
4. ✅ Data appears in ClickHouse `raw.destatis_metadata` table
5. ✅ Logs show successful API responses and data validation

## 📞 Support

If issues arise:

1. Check Airflow logs for detailed error messages
2. Verify infrastructure status: `make status`
3. Test API connectivity: `python test_destatis_pipeline.py`
4. Validate environment: `python validate_destatis_production.py`

---

**Status**: ✅ **PRODUCTION READY**  
**Last Validated**: August 13, 2025  
**Recommended DAG**: `fetch_destatis_metadata_clean.py`
