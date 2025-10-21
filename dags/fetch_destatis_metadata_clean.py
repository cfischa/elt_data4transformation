# -*- coding: utf-8 -*-
"""
Airflow DAG for Destatis metadata extraction.
Fetches all Destatis cube metadata using official GENESIS-Online REST API 2025.

REQUIRED ENVIRONMENT VARIABLES OR AIRFLOW VARIABLES:
- DESTATIS_API_KEY or DESTATIS_TOKEN: API token for authentication
- Alternative: DESTATIS_USER and DESTATIS_PASS for username/password auth
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import logging
import sys
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add project root to path for imports
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

@dag(
    dag_id="fetch_destatis_metadata_production",
    description="Production Destatis cube metadata fetcher using GENESIS-Online REST API 2025",
    schedule="@weekly",  # Run weekly - metadata doesn't change frequently
    start_date=datetime(2025, 1, 1),  # Production start date
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-team",
        "depends_on_past": False,
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["destatis", "metadata", "genesis-api", "production"],
)
def fetch_destatis_metadata_production():
    """Production DAG to fetch and store Destatis cube metadata."""

    def get_destatis_credentials():
        """Get Destatis credentials with robust fallback handling."""
        try:
            # Try Airflow Variables first (production)
            try:
                api_token = Variable.get("DESTATIS_API_KEY", default_var=None)
                if api_token:
                    logger.info("Using Destatis API token from Airflow Variable")
                    return {"type": "token", "value": api_token}
                
                username = Variable.get("DESTATIS_USER", default_var=None)
                password = Variable.get("DESTATIS_PASS", default_var=None)
                if username and password:
                    logger.info("Using Destatis credentials from Airflow Variables")
                    return {"type": "userpass", "username": username, "password": password}
                    
            except Exception as e:
                logger.warning(f"Could not access Airflow Variables: {e}")
            
            # Fallback to environment variables (development)
            api_token = os.getenv('DESTATIS_API_KEY') or os.getenv('DESTATIS_TOKEN')
            if api_token:
                logger.info("Using Destatis API token from environment")
                return {"type": "token", "value": api_token}
            
            username = os.getenv('DESTATIS_USER')
            password = os.getenv('DESTATIS_PASS')
            if username and password:
                logger.info("Using Destatis credentials from environment")
                return {"type": "userpass", "username": username, "password": password}
            
            # Anonymous access for development/testing
            logger.warning("No credentials found - using anonymous access")
            logger.warning("Anonymous access has limitations (rate limits, dataset size)")
            return {"type": "anonymous"}
                
        except Exception as e:
            logger.error(f"Error getting credentials: {e}")
            return {"type": "anonymous"}

    @task(task_id="test_api_connection")
    def test_api_connection():
        """Test API connection with a small sample."""
        import asyncio
        from connectors.destatis_connector import (
            DestatisConnector,
            DestatisConfig,
            ENDPOINT_CATALOGUE_CUBES,
            ENDPOINT_CATALOGUE_TABLES,
        )
        
        async def test_connection():
            try:
                credentials = get_destatis_credentials()
                logger.info(f"Testing with credential type: {credentials.get('type')}")
                # Build connector config explicitly from credentials
                if credentials.get('type') == 'token':
                    cfg = DestatisConfig(api_token=credentials.get('value'))
                elif credentials.get('type') == 'userpass':
                    cfg = DestatisConfig(username=credentials.get('username'), password=credentials.get('password'))
                else:
                    cfg = DestatisConfig()

                async with DestatisConnector(cfg) as connector:
                    # Test with a very small sample
                    cubes = await connector.get_available_cubes(pagelength=3)
                    
                    if cubes:
                        logger.info(f"API test successful: {len(cubes)} cubes retrieved")
                        return {
                            "status": "success", 
                            "sample_count": len(cubes),
                            "credentials": credentials.get('type')
                        }
                    else:
                        return {"status": "failed", "error": "No cubes returned"}
                        
            except Exception as e:
                logger.error(f"API test failed: {e}")
                return {"status": "failed", "error": str(e)}
        
        # Safe asyncio execution for Airflow
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(test_connection())
            finally:
                loop.close()
        except Exception as e:
            return {"status": "failed", "error": f"Async error: {e}"}

    @task(task_id="fetch_all_metadata")
    def fetch_all_metadata(test_result):
        """Fetch all cube metadata if API test passed."""
        if test_result.get("status") != "success":
            raise ValueError(f"API test failed: {test_result.get('error')}")
        
        import asyncio
        from datetime import datetime
        from connectors.destatis_connector import DestatisConnector, DestatisConfig
        from elt.loader_clickhouse import ClickHouseLoader
        
        async def fetch_and_store():
            try:
                # Resolve credentials and build connector config with tuned timeouts
                credentials = get_destatis_credentials()
                common_cfg = dict(timeout=300, rate_limit_requests=25, rate_limit_period=60)
                if credentials.get('type') == 'token':
                    cfg = DestatisConfig(api_token=credentials.get('value'), **common_cfg)
                elif credentials.get('type') == 'userpass':
                    cfg = DestatisConfig(username=credentials.get('username'), password=credentials.get('password'), **common_cfg)
                else:
                    cfg = DestatisConfig(**common_cfg)

                async with DestatisConnector(cfg) as connector:
                    logger.info("🔍 Fetching ALL Destatis cube metadata with prefix-chunked approach...")

                    # Chunk by code prefixes to avoid timeouts and 5xx
                    page_size = 1000
                    prefixes = [str(d) for d in range(10)] + [chr(c) for c in range(ord('A'), ord('Z')+1)]
                    seen = {}

                    saturated_prefixes = []
                    for pref in prefixes:
                        sel = f"{pref}*"
                        try:
                            logger.info(f"Fetching prefix {sel} with page size {page_size}")
                            cubes = await connector.get_available_cubes(selection=sel, pagelength=page_size)
                            for cube in cubes:
                                if cube.code and cube.code not in seen:
                                    seen[cube.code] = cube
                            if len(cubes) >= page_size:
                                saturated_prefixes.append(pref)
                            # Gentle backoff to respect API limits
                            await asyncio.sleep(0.3)
                        except Exception as e:
                            logger.warning(f"Prefix {sel} failed: {e}")

                    # Second pass: refine only saturated prefixes with two-character sub-prefixes
                    if saturated_prefixes:
                        logger.info(f"Refining saturated prefixes: {saturated_prefixes}")
                        for pref in saturated_prefixes:
                            for ch in prefixes:
                                sel2 = f"{pref}{ch}*"
                                try:
                                    logger.info(f"Refine fetch {sel2} with page size {page_size}")
                                    cubes2 = await connector.get_available_cubes(selection=sel2, pagelength=page_size)
                                    for cube in cubes2:
                                        if cube.code and cube.code not in seen:
                                            seen[cube.code] = cube
                                    await asyncio.sleep(0.15)
                                except Exception as e:
                                    logger.warning(f"Sub-prefix {sel2} failed: {e}")

                    all_cubes = list(seen.values())
                    logger.info(f"✅ Retrieved {len(all_cubes)} unique cube metadata records across prefixes")

                    # Transform into metadata records
                    current_time = datetime.now()
                    metadata_records = []
                    for cube in all_cubes:
                        metadata_records.append({
                            "cube_code": cube.code or "",
                            "content": cube.content or "",
                            "state": cube.state or "",
                            "time_coverage": cube.time_coverage or "",
                            "latest_update": cube.latest_update,
                            "information": cube.information,
                            "fetched_at": current_time,
                            "source": "destatis_catalogue",
                            "created_at": current_time,
                            "updated_at": current_time,
                        })
                    
                    if not all_cubes:
                        return {"status": "failed", "error": "No cubes retrieved"}
                    
                    # Prepare data for ClickHouse
                    current_time = datetime.now()
                    metadata_records = []
                    
                    for cube in all_cubes:
                        record = {
                            "cube_code": cube.code or "",
                            "content": cube.content or "",
                            "state": cube.state or "",
                            "time_coverage": cube.time_coverage or "",
                            "latest_update": cube.latest_update,
                            "information": cube.information,
                            "fetched_at": current_time,
                            "source": "destatis_catalogue",
                            "created_at": current_time,
                            "updated_at": current_time
                        }
                        metadata_records.append(record)
                    
                    # Save to ClickHouse
                    with ClickHouseLoader() as ch_loader:
                        # Ensure table exists; create from SQL if missing
                        result = ch_loader.client.query("SHOW TABLES FROM raw LIKE 'destatis_metadata'")
                        if not result.result_rows:
                            logger.warning("Table 'raw.destatis_metadata' not found. Creating it now...")
                            sql_path = os.path.join(project_root, 'sql', 'create_destatis_metadata_table.sql')
                            if not os.path.exists(sql_path):
                                raise FileNotFoundError(f"Missing DDL file: {sql_path}")
                            with open(sql_path, 'r', encoding='utf-8') as f:
                                ddl = f.read()
                            # Split on semicolons and execute non-empty statements
                            for stmt in [s.strip() for s in ddl.split(';') if s.strip()]:
                                ch_loader.client.command(stmt)
                            # Re-check
                            result = ch_loader.client.query("SHOW TABLES FROM raw LIKE 'destatis_metadata'")
                            if not result.result_rows:
                                raise ValueError("Failed to create table 'raw.destatis_metadata'")

                        inserted_count = ch_loader.insert_json_data(
                            table_name="raw.destatis_metadata",
                            json_data=metadata_records,
                            batch_size=1000
                        )
                    
                    logger.info(f"Inserted {inserted_count} metadata records into ClickHouse")
                    
                    return {
                        "status": "success",
                        "cubes_processed": len(all_cubes),
                        "records_inserted": inserted_count,
                        "table": "raw.destatis_metadata",
                        "page_size_used": page_size,
                        "potential_more_data": len(all_cubes) == page_size
                    }
                    
            except Exception as e:
                logger.error(f"Metadata fetch failed: {e}")
                import traceback
                logger.error(f"Full traceback: {traceback.format_exc()}")
                return {"status": "failed", "error": str(e)}
        
        # Safe asyncio execution for Airflow
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(fetch_and_store())
            finally:
                loop.close()
        except Exception as e:
            return {"status": "failed", "error": f"Async error: {e}"}

    @task(task_id="validate_data")
    def validate_data(fetch_result):
        """Validate the inserted data in ClickHouse."""
        if fetch_result.get("status") != "success":
            raise ValueError(f"Fetch failed: {fetch_result.get('error')}")
        
        try:
            from elt.loader_clickhouse import ClickHouseLoader
            
            with ClickHouseLoader() as ch_loader:
                # Count total records
                total_result = ch_loader.client.query("SELECT COUNT(*) FROM raw.destatis_metadata")
                total_count = total_result.result_rows[0][0]
                
                # Count today's records
                today_result = ch_loader.client.query(
                    "SELECT COUNT(*) FROM raw.destatis_metadata WHERE toDate(fetched_at) = today()"
                )
                today_count = today_result.result_rows[0][0]
                
                # Get sample records
                sample_result = ch_loader.client.query(
                    "SELECT cube_code, content FROM raw.destatis_metadata ORDER BY fetched_at DESC LIMIT 3"
                )
                
                logger.info("Validation complete:")
                logger.info(f"  - Total records: {total_count}")
                logger.info(f"  - Today's records: {today_count}")
                logger.info(f"  - Expected from fetch: {fetch_result.get('records_inserted', 0)}")
                
                if sample_result.result_rows:
                    logger.info("Latest records:")
                    for i, row in enumerate(sample_result.result_rows):
                        logger.info(f"  {i+1}. {row[0]}: {row[1][:50]}...")
                
                success = today_count > 0
                return {
                    "validation_status": "success" if success else "failed",
                    "total_records": total_count,
                    "new_records": today_count,
                    "sample_code": sample_result.result_rows[0][0] if sample_result.result_rows else None
                }
                
        except Exception as e:
            logger.error(f"Data validation failed: {e}")
            return {"validation_status": "failed", "error": str(e)}

    @task.short_circuit(task_id="has_new_destatis_metadata")
    def has_new_destatis_metadata(validation_result):
        """Determine if this run ingested fresh Destatis metadata rows."""
        new_records = validation_result.get("new_records", 0)
        if new_records and new_records > 0:
            logger.info("Detected %s new Destatis metadata rows.", new_records)
            return True
        logger.info("No new Destatis metadata detected for this run.")
        return False

    # Define task flow
    test_task = test_api_connection()
    fetch_task = fetch_all_metadata(test_task)
    validate_task = validate_data(fetch_task)
    
    # Set up dependencies
    test_task >> fetch_task >> validate_task

    trigger_guard = has_new_destatis_metadata(validate_task)

    trigger_topic_pipeline = TriggerDagRunOperator(
        task_id="trigger_topic_classifier_pipeline",
        trigger_dag_id="topic_classifier_pipeline",
        conf={
            "sources": ["destatis"],
            "since": "{{ data_interval_start.isoformat() }}",
            "triggered_by": "fetch_destatis_metadata_production",
            "new_metadata_count": "{{ (ti.xcom_pull(task_ids='validate_data', key='return_value') or {}).get('new_records', 0) }}",
        },
        wait_for_completion=False,
    )

    trigger_guard >> trigger_topic_pipeline

# Instantiate the DAG
fetch_destatis_metadata_production()
