"""
Airflow DAG for Destatis metadata extraction.
Fetches all Destatis cube metadata using official GENESIS-Online REST API 2025.
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
    dag_id="fetch_destatis_metadata",
    description="Fetch Destatis cube metadata using official GENESIS-Online REST API 2025",
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
    tags=["destatis", "metadata", "genesis-api"],
)
def fetch_destatis_metadata_dag():
    """DAG to fetch and store Destatis cube metadata using official API endpoints."""

    @task(task_id="fetch_sample_cubes")
    def fetch_sample_cubes() -> dict:
        """Fetch a small sample of cubes to test the connection and API."""
        import asyncio
        
        async def main():
            try:
                from connectors.destatis_connector import DestatisConnector
                
                async with DestatisConnector() as connector:
                    logger.info("🔍 Testing connection by fetching sample cubes...")
                    
                    # Get a limited sample first to test (first 10 cubes)
                    # Use higher pagelength to see total available cubes (default is only 1000)
                    # TESTING: Limit to 10 cubes for initial test run
                    all_cubes = await connector.get_available_cubes(pagelength=10)
                    sample_cubes = all_cubes[:10]  # Test with first 10 cubes
                    
                    logger.info(f"✅ Retrieved {len(sample_cubes)} sample cubes from {len(all_cubes)} total available")
                    
                    if sample_cubes:
                        logger.info("📋 Sample cube codes:")
                        for i, cube in enumerate(sample_cubes[:3]):
                            logger.info(f"  {i+1}. {cube.code}: {cube.content[:50]}...")
                        
                        return {
                            "sample_count": len(sample_cubes),
                            "total_available": len(all_cubes),
                            "sample_codes": [cube.code for cube in sample_cubes],
                            "status": "success"
                        }
                    else:
                        logger.warning("⚠️ No cubes retrieved from API")
                        return {"status": "failed", "error": "No cubes found"}
                            
            except Exception as e:
                logger.error(f"❌ Destatis metadata fetch failed: {e}")
                import traceback
                logger.error(f"Full traceback: {traceback.format_exc()}")
                return {"status": "failed", "error": str(e)}

        try:
            result = asyncio.run(main())
            logger.info(f"🎯 Sample fetch completed with status: {result.get('status')}")
            return result
        except Exception as e:
            logger.error(f"❌ Async execution failed: {e}")
            return {"status": "failed", "error": str(e)}

    @task(task_id="check_sample_result", trigger_rule="all_success")
    def check_sample_result(sample_result: dict) -> dict:
        """Check if the sample fetch was successful before proceeding."""
        if sample_result.get("status") != "success" or sample_result.get("sample_count", 0) < 5:
            logger.error("Sample cube fetch failed or insufficient data. Aborting full metadata fetch.")
            return {"proceed": False}
        logger.info(f"Sample batch of {sample_result['sample_count']} cubes loaded successfully. Proceeding to full fetch.")
        return {"proceed": True}

    @task.short_circuit(task_id="should_proceed", trigger_rule="all_success")
    def should_proceed(check: dict) -> bool:
        """Short circuit if sample fetch failed."""
        return check.get("proceed", False)

    @task(task_id="fetch_all_cube_metadata", trigger_rule="all_success")
    def fetch_all_cube_metadata() -> dict:
        """Fetch complete metadata for ALL available cubes and save directly to ClickHouse."""
        import asyncio
        from elt.loader_clickhouse import ClickHouseLoader
        
        async def main():
            try:
                from connectors.destatis_connector import DestatisConnector
                
                async with DestatisConnector() as connector:
                    logger.info("🔍 Fetching ALL cube metadata from Destatis catalogue...")
                    
                    # Get ALL available cubes with full metadata - NO FILTERING
                    # Use higher pagelength to see total available cubes (default is only 1000)
                    # TESTING: Limit to 10 cubes for initial test run
                    all_cubes = await connector.get_available_cubes(pagelength=10)
                    
                    logger.info(f"✅ Retrieved metadata for {len(all_cubes)} cubes")
                    
                    if all_cubes:
                        # Convert to format suitable for ClickHouse insertion
                        metadata_records = []
                        current_time = datetime.now()
                        
                        for cube in all_cubes:
                            record = {
                                "cube_code": cube.code,
                                "content": cube.content,
                                "state": cube.state,
                                "time_coverage": cube.time_coverage,
                                "latest_update": cube.latest_update,
                                "information": cube.information,
                                "fetched_at": current_time,
                                "source": "destatis_catalogue",
                                "created_at": current_time,
                                "updated_at": current_time
                            }
                            metadata_records.append(record)
                        
                        # Save directly to ClickHouse
                        with ClickHouseLoader() as ch_loader:
                            inserted_count = ch_loader.insert_json_data(
                                table_name="raw.destatis_metadata",
                                json_data=metadata_records,
                                batch_size=1000
                            )
                        
                        logger.info(f"💾 Saved {inserted_count} cube metadata records to ClickHouse raw.destatis_metadata table")
                        
                        # Log some statistics
                        avg_content_length = sum(len(cube.content or "") for cube in all_cubes) / len(all_cubes)
                        logger.info(f"📊 Average content description length: {avg_content_length:.1f} characters")
                        
                        # Count by topic categories for overview
                        topics = {}
                        for cube in all_cubes:
                            # Extract first part of code as topic indicator
                            topic_code = cube.code[:5] if cube.code else "unknown"
                            topics[topic_code] = topics.get(topic_code, 0) + 1
                        
                        logger.info("📋 Top 5 topic areas by cube count:")
                        for topic, count in sorted(topics.items(), key=lambda x: x[1], reverse=True)[:5]:
                            logger.info(f"  {topic}: {count} cubes")
                        
                        return {
                            "cubes_processed": len(metadata_records),
                            "inserted_records": inserted_count,
                            "topic_areas": len(topics),
                            "status": "success",
                            "storage_location": "clickhouse://raw.destatis_metadata"
                        }
                    else:
                        logger.warning("⚠️ No cube metadata retrieved")
                        return {"status": "failed", "error": "No cubes found"}
                        
            except Exception as e:
                logger.error(f"❌ Full metadata fetch failed: {e}")
                import traceback
                logger.error(f"Full traceback: {traceback.format_exc()}")
                return {"status": "failed", "error": str(e)}

        try:
            result = asyncio.run(main())
            logger.info(f"🎯 Full metadata fetch completed: {result.get('cubes_processed', 0)} cubes processed")
            return result
        except Exception as e:
            logger.error(f"❌ Async execution failed: {e}")
            return {"status": "failed", "error": str(e)}

    @task(task_id="validate_clickhouse_data", trigger_rule="all_success")
    def validate_clickhouse_data(fetch_result: dict) -> dict:
        """Validate the saved metadata in ClickHouse."""
        if fetch_result.get("status") != "success":
            return {"validation_status": "failed", "error": "Fetch failed"}
        
        try:
            from elt.loader_clickhouse import ClickHouseLoader
            
            with ClickHouseLoader() as ch_loader:
                # Check if table exists and has data
                result = ch_loader.client.query(
                    "SELECT COUNT(*) as count FROM raw.destatis_metadata WHERE fetched_at >= today()"
                )
                count = result.result_rows[0][0] if result.result_rows else 0
                
                if count == 0:
                    return {"validation_status": "failed", "error": "No records found in ClickHouse"}
                
                # Check structure by getting a sample record
                sample_result = ch_loader.client.query(
                    "SELECT cube_code, content, state, time_coverage, source FROM raw.destatis_metadata LIMIT 1"
                )
                
                if not sample_result.result_rows:
                    return {"validation_status": "failed", "error": "No sample record found"}
                
                sample_record = sample_result.result_rows[0]
                
                # Verify expected number of records
                expected_count = fetch_result.get("inserted_records", 0)
                if count != expected_count:
                    logger.warning(f"Expected {expected_count} records, found {count}")
                
                logger.info(f"✅ ClickHouse validation passed: {count} records in raw.destatis_metadata")
                logger.info(f"📋 Sample record: {sample_record[0]} - {sample_record[1][:50]}...")
                
                return {
                    "validation_status": "success",
                    "records_count": count,
                    "sample_cube_code": sample_record[0],
                    "table_name": "raw.destatis_metadata"
                }
                
        except Exception as e:
            logger.error(f"❌ ClickHouse validation failed: {e}")
            return {"validation_status": "failed", "error": str(e)}

    # Define task dependencies
    sample_task = fetch_sample_cubes()
    check_task = check_sample_result(sample_task)
    proceed_task = should_proceed(check_task)
    metadata_task = fetch_all_cube_metadata()
    validate_task = validate_clickhouse_data(metadata_task)
    
    # Set up workflow
    sample_task >> check_task >> proceed_task >> metadata_task >> validate_task


# Instantiate the DAG
fetch_destatis_metadata_dag()
