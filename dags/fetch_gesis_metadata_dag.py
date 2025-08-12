"""
Airflow DAG for GESIS metadata extraction.
Fetches all GESIS dataset metadata and saves to meta_data/gesis_metadata.json.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
import logging
import sys
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add project root to path for imports
sys.path.append('/opt/airflow')

@dag(
    dag_id="fetch_gesis_metadata",
    description="Fetch GESIS metadata and save to meta_data/gesis_metadata.json",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-team",
        "depends_on_past": False,
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["gesis", "metadata"],
)
def fetch_gesis_metadata_dag():
    """DAG to fetch and store GESIS dataset metadata regularly."""



    @task(task_id="fetch_10_metadata")
    def fetch_10_metadata() -> list:
        import asyncio
        
        async def main():
            try:
                # Test if SPARQLWrapper is available
                try:
                    from SPARQLWrapper import SPARQLWrapper, JSON
                    logger.info("✅ SPARQLWrapper is available")
                except ImportError as e:
                    logger.error(f"❌ SPARQLWrapper not available: {e}")
                    logger.info("Installing SPARQLWrapper...")
                    import subprocess
                    subprocess.check_call([sys.executable, "-m", "pip", "install", "SPARQLWrapper>=2.0.0"])
                    from SPARQLWrapper import SPARQLWrapper, JSON
                    logger.info("✅ SPARQLWrapper installed")
                
                # Use the real connector for limited test
                from connectors.gesis_connector import GESISConnector
                
                async with GESISConnector() as connector:
                    logger.info("🔍 Using SELECT query to fetch 10 datasets with all metadata...")
                    
                    # Use the new limited method for testing
                    all_metadata = await connector.get_limited_datasets_metadata(limit=10)
                    
                    logger.info(f"✅ Retrieved {len(all_metadata)} datasets with complete metadata")
                    
                    if all_metadata:
                        avg_props = sum(len(d.get('properties', {})) for d in all_metadata) / len(all_metadata)
                        logger.info(f"📊 Average properties per dataset: {avg_props:.1f}")
                        logger.info(f"📋 Sample metadata entry ID: {all_metadata[0].get('id', 'NO ID')}")
                        logger.info(f"📋 Sample metadata title: {all_metadata[0].get('title', 'NO TITLE')}")
                        return all_metadata
                    else:
                        logger.warning("⚠️ No metadata retrieved")
                        return []
                            
            except Exception as e:
                logger.error(f"❌ GESIS metadata fetch failed completely: {e}")
                import traceback
                logger.error(f"Full traceback: {traceback.format_exc()}")
                return []

        try:
            result = asyncio.run(main())
            logger.info(f"🎯 Task completed. Retrieved {len(result)} metadata entries.")
            return result
        except Exception as e:
            logger.error(f"❌ Async execution failed: {e}")
            return []

    @task(task_id="check_test_result", trigger_rule="all_success", multiple_outputs=True)
    def check_test_result(success: dict) -> dict:
        if not success or success.get("rows_loaded", 0) < 10:
            logger.error("Initial test load of 10 metadata failed or incomplete. Aborting full fetch.")
            return {"proceed": False}
        logger.info("Test batch loaded successfully. Proceeding to full fetch.")
        return {"proceed": True}

    @task.short_circuit(task_id="should_proceed", trigger_rule="all_success")
    def should_proceed(check: dict) -> bool:
        return check.get("proceed", False)

    @task(task_id="fetch_all_metadata", trigger_rule="all_success")
    def fetch_all_metadata() -> list:
        import asyncio
        from connectors.gesis_connector import GESISConnector

        async def main():
            async with GESISConnector() as connector:
                logger.info("🔍 Using SELECT query to fetch ALL datasets with complete metadata...")
                
                # Use optimized batch size for full dataset extraction
                # batch_size=20 to avoid URI length limits, no limit to get all 9,176+ datasets
                all_metadata = await connector.get_all_datasets_metadata(batch_size=20, limit=None)
                
                logger.info(f"✅ Retrieved {len(all_metadata)} datasets with complete metadata")
                
                if all_metadata:
                    avg_props = sum(len(d.get('properties', {})) for d in all_metadata) / len(all_metadata)
                    logger.info(f"📊 Average properties per dataset: {avg_props:.1f}")
                    logger.info(f"📋 Sample metadata entry ID: {all_metadata[0].get('id', 'NO ID')}")
                    logger.info(f"📋 Sample metadata title: {all_metadata[0].get('title', 'NO TITLE')}")
                    
                    # Log statistics about data completeness
                    with_titles = sum(1 for d in all_metadata if d.get('title'))
                    with_descriptions = sum(1 for d in all_metadata if d.get('description'))
                    logger.info(f"📈 Data completeness: {with_titles} titles ({100*with_titles/len(all_metadata):.1f}%), {with_descriptions} descriptions ({100*with_descriptions/len(all_metadata):.1f}%)")
                
                return all_metadata

        try:
            return asyncio.run(main())
        except Exception as e:
            logger.error(f"GESIS metadata fetch failed: {e}")
            raise

    @task(task_id="load_metadata_clickhouse")
    def load_metadata_clickhouse(all_metadata: list) -> dict:
        from elt.loader_clickhouse import ClickHouseLoader, ClickHouseConfig
        import os
        
        if not all_metadata:
            logger.warning("No metadata to load into ClickHouse.")
            return {"rows_loaded": 0, "status": "success", "message": "No data to load"}
        
        # Filter out any None or empty entries
        valid_metadata = [item for item in all_metadata if item and isinstance(item, dict)]
        
        if not valid_metadata:
            logger.warning("No valid metadata entries found.")
            return {"rows_loaded": 0, "status": "success", "message": "No valid data to load"}
        
        try:
            config = ClickHouseConfig(
                host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
                port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
                username=os.getenv("CLICKHOUSE_USER", "admin"),
                password=os.getenv("CLICKHOUSE_PASSWORD", ""),
                database=os.getenv("CLICKHOUSE_DATABASE", "analytics")
            )
            
            with ClickHouseLoader(config) as loader:
                rows_loaded = loader.upsert_gesis_metadata(valid_metadata)
                logger.info(f"Successfully loaded {rows_loaded} GESIS metadata records to ClickHouse")
                return {
                    "rows_loaded": rows_loaded,
                    "status": "success",
                    "message": f"Loaded {rows_loaded} metadata records",
                    "table": "raw.gesis_metadata"
                }
        except Exception as e:
            logger.error(f"Failed to load GESIS metadata to ClickHouse: {e}")
            return {
                "rows_loaded": 0,
                "status": "failed",
                "message": f"Error: {str(e)}"
            }

    # Task dependencies
    test_meta = fetch_10_metadata()
    test_result = load_metadata_clickhouse(test_meta)
    check = check_test_result(test_result)
    proceed = should_proceed(check)
    all_meta = fetch_all_metadata()
    proceed >> all_meta
    load_metadata_clickhouse(all_meta)

# Instantiate the DAG
gesis_metadata_dag = fetch_gesis_metadata_dag()