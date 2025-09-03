"""
Production-ready Airflow DAG for Destatis metadata extraction.
Fetches all Destatis cube metadata using official GENESIS-Online REST API 2025.

This DAG is optimized for production with:
- Robust error handling and logging
- Automatic credential management
- Anonymous access fallback for development
- Proper async handling in Airflow context
- Data validation and monitoring
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

# Add project root to path for imports (works in both local dev and Docker)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

@dag(
    dag_id="fetch_destatis_metadata_v2",
    description="Production-ready Destatis cube metadata fetcher using GENESIS-Online REST API 2025",
    schedule="@weekly",  # Run weekly - metadata doesn't change frequently
    start_date=datetime(2025, 1, 1),  # Production start date
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-team",
        "depends_on_past": False,
        "email_on_failure": True,
        "email_on_retry": False,  # Reduce noise for development
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
                    logger.info("✅ Using Destatis API token from Airflow Variable")
                    return {"type": "token", "value": api_token}
                
                username = Variable.get("DESTATIS_USER", default_var=None)
                password = Variable.get("DESTATIS_PASS", default_var=None)
                if username and password:
                    logger.info("✅ Using Destatis credentials from Airflow Variables")
                    return {"type": "userpass", "username": username, "password": password}
                    
            except Exception as e:
                logger.warning(f"Could not access Airflow Variables: {e}")
            
            # Fallback to environment variables (development)
            api_token = os.getenv('DESTATIS_API_KEY') or os.getenv('DESTATIS_TOKEN')
            if api_token:
                logger.info("✅ Using Destatis API token from environment")
                return {"type": "token", "value": api_token}
            
            username = os.getenv('DESTATIS_USER')
            password = os.getenv('DESTATIS_PASS')
            if username and password:
                logger.info("✅ Using Destatis credentials from environment")
                return {"type": "userpass", "username": username, "password": password}
            
            # Anonymous access for development/testing
            logger.warning("⚠️ No credentials found - using anonymous access")
            logger.warning("Anonymous access has limitations (rate limits, dataset size)")
            return {"type": "anonymous"}
                
        except Exception as e:
            logger.error(f"❌ Error getting credentials: {e}")
            return {"type": "anonymous"}

    @task(task_id="test_api_connection")
    def test_api_connection() -> dict:
        """Test API connection with a small sample."""
        import asyncio
        from connectors.destatis_connector import DestatisConnector
        
        async def test_connection():
            try:
                credentials = get_destatis_credentials()
                logger.info(f"Testing with credential type: {credentials.get('type')}")
                
                async with DestatisConnector() as connector:
                    # Test with a very small sample
                    cubes = await connector.get_available_cubes(pagelength=3)
                    
                    if cubes:
                        logger.info(f"✅ API test successful: {len(cubes)} cubes retrieved")
                        return {
                            "status": "success", 
                            "sample_count": len(cubes),
                            "credentials": credentials.get('type')
                        }
                    else:
                        return {"status": "failed", "error": "No cubes returned"}
                        
            except Exception as e:
                logger.error(f"❌ API test failed: {e}")
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
    def fetch_all_metadata(test_result: dict) -> dict:
        """Fetch all cube metadata if API test passed."""
        if test_result.get("status") != "success":
            raise ValueError(f"API test failed: {test_result.get('error')}")
        
        import asyncio
        from datetime import datetime
        from connectors.destatis_connector import DestatisConnector
        from elt.loader_clickhouse import ClickHouseLoader
        
        async def fetch_and_store():
            try:
                async with DestatisConnector() as connector:
                    logger.info("🔍 Fetching ALL Destatis cube metadata...")
                    
                    # Get ALL available cubes with full metadata
                    all_cubes = await connector.get_available_cubes(pagelength=50000)
                    logger.info(f"✅ Retrieved {len(all_cubes)} cube metadata records")
                    
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
                        # Verify table exists
                        result = ch_loader.client.query("SHOW TABLES FROM raw LIKE 'destatis_metadata'")
                        if not result.result_rows:
                            raise ValueError("Table 'raw.destatis_metadata' does not exist")
                        
                        inserted_count = ch_loader.insert_json_data(
                            table_name="raw.destatis_metadata",
                            json_data=metadata_records,
                            batch_size=1000
                        )
                    
                    logger.info(f"💾 Inserted {inserted_count} metadata records into ClickHouse")
                    
                    return {
                        "status": "success",
                        "cubes_processed": len(all_cubes),
                        "records_inserted": inserted_count,
                        "table": "raw.destatis_metadata"
                    }
                    
            except Exception as e:
                logger.error(f"❌ Metadata fetch failed: {e}")
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
    def validate_data(fetch_result: dict) -> dict:
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
                
                logger.info(f"📊 Validation complete:")
                logger.info(f"  - Total records: {total_count}")
                logger.info(f"  - Today's records: {today_count}")
                logger.info(f"  - Expected from fetch: {fetch_result.get('records_inserted', 0)}")
                
                if sample_result.result_rows:
                    logger.info("📋 Latest records:")
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
            logger.error(f"❌ Data validation failed: {e}")
            return {"validation_status": "failed", "error": str(e)}

    # Define task flow
    test_task = test_api_connection()
    fetch_task = fetch_all_metadata(test_task)
    validate_task = validate_data(fetch_task)
    
    # Set up dependencies
    test_task >> fetch_task >> validate_task

# Instantiate the DAG
fetch_destatis_metadata_production()
