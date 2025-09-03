# 🚀 Destatis Metadata DAG - Production Deployment Guide

## Overview
This guide covers the production deployment of the `fetch_destatis_metadata` DAG for fetching Destatis cube metadata using the official GENESIS-Online REST API 2025.

## ✅ Pre-Deployment Checklist

### 1. Environment Configuration
- [ ] ClickHouse server is running and accessible
- [ ] Database `raw` exists in ClickHouse
- [ ] Table `raw.destatis_metadata` is created (use `sql/create_destatis_metadata_table.sql`)
- [ ] Destatis API credentials are configured in environment variables
- [ ] Airflow environment has all required Python packages installed

### 2. API Configuration
- [ ] Destatis API token is valid and not expired
- [ ] API endpoint `https://www-genesis.destatis.de/genesisWS/rest/2020/catalogue/cubes` is accessible
- [ ] Network connectivity allows HTTPS requests to destatis.de

### 3. Dependencies
- [ ] Python packages: `httpx`, `clickhouse-connect`, `pandas`, `pydantic`
- [ ] Custom modules: `connectors.destatis_connector`, `elt.loader_clickhouse`
- [ ] Airflow version 2.0+ with TaskFlow API support

## 🔧 Configuration Details

### DAG Configuration
```python
# Production Settings
schedule: "@weekly"           # Runs every Sunday at midnight
max_active_runs: 1           # Prevent concurrent executions
retries: 2                   # Retry failed tasks twice
retry_delay: 10 minutes      # Wait between retries
pagelength: 50000           # Fetch ALL available cubes
```

### Expected Data Volume
- **Estimated Records**: 10,000-50,000 cube metadata records
- **Processing Time**: 5-15 minutes depending on API response time
- **Storage Requirements**: ~50-200 MB per run in ClickHouse

## 🚦 Deployment Steps

### Step 1: Verify Prerequisites
```bash
# Test ClickHouse connection
python -c "from elt.loader_clickhouse import ClickHouseLoader; loader = ClickHouseLoader(); print('✅ ClickHouse OK')"

# Test Destatis API connection
python test_destatis_pipeline.py
```

### Step 2: Deploy DAG
1. Copy `fetch_destatis_metadata_dag.py` to Airflow DAGs folder
2. Ensure all connector modules are in the Python path
3. Verify DAG appears in Airflow UI without import errors

### Step 3: Initial Run
1. Trigger DAG manually for first production run
2. Monitor execution in Airflow UI
3. Verify data in ClickHouse: `SELECT COUNT(*) FROM raw.destatis_metadata`

### Step 4: Set Up Monitoring
1. Configure Airflow email notifications
2. Set up data quality checks
3. Monitor ClickHouse table growth

## 📊 Monitoring & Maintenance

### Key Metrics to Monitor
- **DAG Success Rate**: Should be >95%
- **Execution Time**: Baseline 5-15 minutes
- **Data Freshness**: New records should appear weekly
- **API Response Time**: Monitor for Destatis API slowdowns

### Common Issues & Solutions
| Issue | Symptoms | Solution |
|-------|----------|----------|
| API Token Expired | 401 Authentication errors | Renew Destatis API token |
| Network Timeout | Connection timeouts | Check network connectivity |
| ClickHouse Full | Insert failures | Monitor disk space |
| Duplicate Data | Same records inserted | Check ReplacingMergeTree deduplication |

### Maintenance Tasks
- **Monthly**: Review API usage and quotas
- **Quarterly**: Update API token if needed
- **Semi-annually**: Review data retention policies

## 🔒 Security Considerations

### Credentials Management
- Store Destatis API credentials in Airflow Variables or Connections
- Use environment variables for sensitive configuration
- Rotate API tokens regularly

### Data Access
- Limit ClickHouse access to authorized users
- Monitor data access patterns
- Implement proper backup procedures

## 🎯 Success Criteria

The deployment is successful when:
- [ ] DAG runs weekly without failures
- [ ] Metadata records are updated in ClickHouse
- [ ] Data quality checks pass
- [ ] No authentication or connection errors
- [ ] Monitoring alerts are configured

## 📞 Support & Troubleshooting

### Log Locations
- **Airflow Logs**: Check task logs in Airflow UI
- **ClickHouse Logs**: System logs for database operations
- **Application Logs**: Custom logging in connector modules

### Contact Information
- **Data Team**: For DAG-related issues
- **Infrastructure Team**: For ClickHouse or network issues
- **API Support**: Destatis GENESIS-Online support

---

## 🏆 Production Readiness Verification

Run this final verification before going live:

```bash
# Complete pipeline test
python test_destatis_pipeline.py

# Verify ClickHouse table structure
clickhouse-client --query "DESCRIBE raw.destatis_metadata"

# Check data quality
clickhouse-client --query "SELECT COUNT(*), MIN(updated_at), MAX(updated_at) FROM raw.destatis_metadata"
```

**Status**: ✅ Ready for Production Deployment
