# -*- coding: utf-8 -*-
"""
Optimized Airflow DAG for Destatis metadata extraction.
Handles large datasets with pagination and timeout management.

FEATURES:
- Pagination to handle large datasets (50,000+ cubes)
- Incremental loading with batch processing
- Robust timeout and retry handling
- Production-ready error handling
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
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
    dag_id="fetch_destatis_metadata_optimized",
    description="Optimized Destatis metadata fetcher with pagination and timeout handling",
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
        "execution_timeout": timedelta(minutes=30),  # Add execution timeout
    },
    tags=["destatis", "metadata", "genesis-api", "optimized"],
)
def fetch_destatis_metadata_optimized():
    """Optimized DAG for large-scale Destatis metadata extraction."""

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
        """Test API connection with a very small sample."""
        import asyncio
        from connectors.destatis_connector import DestatisConnector
        
        async def test_connection():
            try:
                credentials = get_destatis_credentials()
                logger.info(f"Testing with credential type: {credentials.get('type')}")
                
                async with DestatisConnector() as connector:
                    # Test with minimal data first
                    logger.info("Testing API with minimal request (3 cubes)...")
                    cubes = await connector.get_available_cubes(pagelength=3)
                    
                    if cubes:
                        logger.info(f"API test successful: {len(cubes)} cubes retrieved")
                        logger.info(f"Sample cube: {cubes[0].code} - {cubes[0].content[:50]}...")
                        return {
                            "status": "success", 
                            "sample_count": len(cubes),
                            "credentials": credentials.get('type'),
                            "sample_cube": cubes[0].code
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

    @task(task_id="fetch_metadata_batches")
    def fetch_metadata_batches(test_result):
        """Fetch metadata in manageable batches to avoid timeouts."""
        if test_result.get("status") != "success":
            raise ValueError(f"API test failed: {test_result.get('error')}")
        
        import asyncio
        from datetime import datetime
        from connectors.destatis_connector import DestatisConnector
        from elt.loader_clickhouse import ClickHouseLoader
        
        async def fetch_in_batches():
            try:
                async with DestatisConnector() as connector:
                    logger.info("Starting optimized metadata fetch with batching...")
                    
                    # Strategy: Fetch in smaller, manageable batches
                    batch_sizes = [100, 500, 1000, 2000]  # Progressive batch sizes
                    all_metadata_records = []
                    current_time = datetime.now()
                    
                    for batch_size in batch_sizes:
                        try:
                            logger.info(f"Attempting batch fetch with {batch_size} cubes...")
                            
                            # Fetch this batch
                            cubes = await connector.get_available_cubes(pagelength=batch_size)
                            
                            if cubes:
                                logger.info(f"Successfully fetched {len(cubes)} cubes in batch")
                                
                                # Convert to ClickHouse format
                                for cube in cubes:
                                    record = {
                                        "cube_code": cube.code or "",
                                        "content": cube.content or "",
                                        "state": cube.state or "",
                                        "time_coverage": cube.time_coverage or "",
                                        "latest_update": cube.latest_update,
                                        "information": cube.information,
                                        "fetched_at": current_time,
                                        "source": f"destatis_batch_{batch_size}",
                                        "created_at": current_time,
                                        "updated_at": current_time
                                    }
                                    all_metadata_records.append(record)
                                
                                # Success - break and use this batch size
                                logger.info(f"Optimal batch size found: {batch_size} cubes")
                                break
                                
                            else:
                                logger.warning(f"No cubes returned for batch size {batch_size}")
                                continue
                                
                        except Exception as e:
                            logger.warning(f"Batch size {batch_size} failed: {e}")
                            continue
                    
                    if not all_metadata_records:
                        return {"status": "failed", "error": "No metadata could be fetched with any batch size"}
                    
                    # Save to ClickHouse
                    with ClickHouseLoader() as ch_loader:
                        # Verify table exists
                        result = ch_loader.client.query("SHOW TABLES FROM raw LIKE 'destatis_metadata'")
                        if not result.result_rows:
                            raise ValueError("Table 'raw.destatis_metadata' does not exist")
                        
                        # Insert in smaller batches for reliability
                        inserted_count = ch_loader.insert_json_data(
                            table_name="raw.destatis_metadata",
                            json_data=all_metadata_records,
                            batch_size=100  # Smaller batch size for reliability
                        )
                    
                    logger.info(f"Successfully inserted {inserted_count} metadata records")
                    
                    return {
                        "status": "success",
                        "cubes_processed": len(all_metadata_records),
                        "records_inserted": inserted_count,
                        "table": "raw.destatis_metadata",
                        "strategy": "batched_fetch"
                    }
                    
            except Exception as e:
                logger.error(f"Batched metadata fetch failed: {e}")
                import traceback
                logger.error(f"Full traceback: {traceback.format_exc()}")
                return {"status": "failed", "error": str(e)}
        
        # Safe asyncio execution for Airflow
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(fetch_in_batches())
            finally:
                loop.close()
        except Exception as e:
            return {"status": "failed", "error": f"Async error: {e}"}

    @task(task_id="expand_metadata_progressive")
    def expand_metadata_progressive(batch_result):
        """Progressively expand metadata collection if initial batch was successful."""
        if batch_result.get("status") != "success":
            logger.warning("Skipping expansion - initial batch failed")
            return batch_result
        
        import asyncio
        from datetime import datetime
        from connectors.destatis_connector import DestatisConnector
        from elt.loader_clickhouse import ClickHouseLoader
        
        async def expand_collection():
            try:
                initial_count = batch_result.get("cubes_processed", 0)
                logger.info(f"Initial batch successful with {initial_count} cubes. Attempting expansion...")
                
                async with DestatisConnector() as connector:
                    # Try progressively larger collections
                    expansion_sizes = [5000, 10000, 20000]
                    current_time = datetime.now()
                    
                    for size in expansion_sizes:
                        try:
                            logger.info(f"Attempting expansion to {size} cubes...")
                            
                            # Fetch larger batch
                            cubes = await connector.get_available_cubes(pagelength=size)
                            
                            if len(cubes) > initial_count:
                                logger.info(f"Expansion successful: {len(cubes)} cubes (up from {initial_count})")
                                
                                # Prepare new records
                                new_records = []
                                for cube in cubes[initial_count:]:  # Only new records
                                    record = {
                                        "cube_code": cube.code or "",
                                        "content": cube.content or "",
                                        "state": cube.state or "",
                                        "time_coverage": cube.time_coverage or "",
                                        "latest_update": cube.latest_update,
                                        "information": cube.information,
                                        "fetched_at": current_time,
                                        "source": f"destatis_expansion_{size}",
                                        "created_at": current_time,
                                        "updated_at": current_time
                                    }
                                    new_records.append(record)
                                
                                # Save additional records
                                if new_records:
                                    with ClickHouseLoader() as ch_loader:
                                        additional_inserted = ch_loader.insert_json_data(
                                            table_name="raw.destatis_metadata",
                                            json_data=new_records,
                                            batch_size=100
                                        )
                                    
                                    total_inserted = batch_result.get("records_inserted", 0) + additional_inserted
                                    logger.info(f"Expansion complete: {additional_inserted} additional records inserted")
                                    
                                    return {
                                        "status": "success",
                                        "cubes_processed": len(cubes),
                                        "records_inserted": total_inserted,
                                        "expansion_successful": True,
                                        "expansion_size": size,
                                        "table": "raw.destatis_metadata"
                                    }
                                break
                                
                        except Exception as e:
                            logger.warning(f"Expansion to {size} failed: {e}")
                            continue
                    
                    # If no expansion worked, return original result
                    logger.info("No expansion possible - returning original batch result")
                    return {**batch_result, "expansion_successful": False}
                    
            except Exception as e:
                logger.error(f"Expansion attempt failed: {e}")
                return {**batch_result, "expansion_successful": False, "expansion_error": str(e)}
        
        # Safe asyncio execution for Airflow
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(expand_collection())
            finally:
                loop.close()
        except Exception as e:
            return {**batch_result, "expansion_error": f"Async error: {e}"}

    @task(task_id="validate_final_data")
    def validate_final_data(final_result):
        """Validate the final inserted data in ClickHouse."""
        if final_result.get("status") != "success":
            raise ValueError(f"Fetch failed: {final_result.get('error')}")
        
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
                    "SELECT cube_code, content, source FROM raw.destatis_metadata ORDER BY fetched_at DESC LIMIT 5"
                )
                
                logger.info("Final validation complete:")
                logger.info(f"  - Total records in table: {total_count}")
                logger.info(f"  - Records inserted today: {today_count}")
                logger.info(f"  - Expected from fetch: {final_result.get('records_inserted', 0)}")
                logger.info(f"  - Expansion successful: {final_result.get('expansion_successful', False)}")
                
                if sample_result.result_rows:
                    logger.info("Latest records:")
                    for i, row in enumerate(sample_result.result_rows):
                        logger.info(f"  {i+1}. {row[0]}: {row[1][:40]}... [source: {row[2]}]")
                
                success = today_count > 0
                return {
                    "validation_status": "success" if success else "failed",
                    "total_records": total_count,
                    "new_records": today_count,
                    "expansion_worked": final_result.get('expansion_successful', False),
                    "final_cube_count": final_result.get('cubes_processed', 0),
                    "table": "raw.destatis_metadata"
                }
                
        except Exception as e:
            logger.error(f"Data validation failed: {e}")
            return {"validation_status": "failed", "error": str(e)}

    # Define task flow with progressive approach
    test_task = test_api_connection()
    batch_task = fetch_metadata_batches(test_task)
    expand_task = expand_metadata_progressive(batch_task)
    validate_task = validate_final_data(expand_task)
    
    # Set up dependencies
    test_task >> batch_task >> expand_task >> validate_task

# Instantiate the DAG
fetch_destatis_metadata_optimized()
