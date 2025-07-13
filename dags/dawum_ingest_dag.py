"""
Airflow DAG for DAWUM polling data ingestion.
Extracts polling data from DAWUM API and loads into ClickHouse raw table.
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List

from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dag(
    dag_id="dawum_ingest",
    description="Extract and load DAWUM polling data",
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
        "retry_delay": timedelta(minutes=5),
    },
    tags=["dawum", "polling", "extraction", "raw"],
)
def dawum_ingest_dag():
    """DAWUM polling data ingestion pipeline."""
    
    @task(task_id="extract_dawum_polls")
    def extract_polls() -> List[Dict[str, Any]]:
        """Extract polling data from DAWUM API."""
        import asyncio
        import sys
        import os
        
        # Add project root to path
        sys.path.append('/opt/airflow')
        
        from connectors.dawum_connector import DawumConnector, DawumConfig
        
        try:
            # Initialize connector with configuration
            config = DawumConfig(
                base_url=os.getenv("DAWUM_BASE_URL", "https://api.dawum.de"),
                api_key=os.getenv("DAWUM_API_KEY"),
                rate_limit_requests=60,
                rate_limit_period=60,
                timeout=30,
                max_retries=3
            )
            
            connector = DawumConnector(config)
            
            # Fetch polls asynchronously
            logger.info("Starting DAWUM polls extraction")
            polls = asyncio.run(connector.fetch_polls())
            
            logger.info(f"Successfully extracted {len(polls)} polls from DAWUM")
            return polls
            
        except Exception as e:
            logger.error(f"Failed to extract DAWUM polls: {e}")
            raise
    
    @task(task_id="load_to_clickhouse")
    def load_polls(polls: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Load polling data into ClickHouse raw table."""
        import sys
        import os
        
        sys.path.append('/opt/airflow')
        
        from elt.loader_clickhouse import ClickHouseLoader, ClickHouseConfig
        
        if not polls:
            logger.warning("No polls data to load")
            return {"rows_loaded": 0, "status": "success", "message": "No data to load"}
        
        try:
            # Initialize ClickHouse loader
            config = ClickHouseConfig(
                host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
                port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
                username=os.getenv("CLICKHOUSE_USER", "default"),
                password=os.getenv("CLICKHOUSE_PASSWORD", ""),
                database=os.getenv("CLICKHOUSE_DATABASE", "analytics")
            )
            
            with ClickHouseLoader(config) as loader:
                # Load data using upsert strategy
                rows_loaded = loader.upsert_dawum_polls(polls)
                
                logger.info(f"Successfully loaded {rows_loaded} poll records to ClickHouse")
                
                return {
                    "rows_loaded": rows_loaded,
                    "status": "success",
                    "message": f"Loaded {rows_loaded} poll records",
                    "table": "raw.dawum_polls"
                }
                
        except Exception as e:
            logger.error(f"Failed to load polls to ClickHouse: {e}")
            raise
    
    @task(task_id="log_success", trigger_rule=TriggerRule.ALL_SUCCESS)
    def log_ingestion_success(load_result: Dict[str, Any]) -> None:
        """Log successful ingestion with summary statistics."""
        logger.info("=== DAWUM Ingestion Summary ===")
        logger.info(f"Status: {load_result['status']}")
        logger.info(f"Rows loaded: {load_result['rows_loaded']}")
        logger.info(f"Target table: {load_result['table']}")
        logger.info(f"Message: {load_result['message']}")
        logger.info("=== End Summary ===")
        
        # Push summary to XCom for monitoring
        return {
            "ingestion_date": datetime.utcnow().isoformat(),
            "source": "dawum",
            "rows_loaded": load_result["rows_loaded"],
            "status": "success"
        }
    
    @task(task_id="log_failure", trigger_rule=TriggerRule.ONE_FAILED)
    def log_ingestion_failure() -> None:
        """Log failed ingestion for monitoring."""
        logger.error("DAWUM ingestion pipeline failed")
        return {
            "ingestion_date": datetime.utcnow().isoformat(),
            "source": "dawum",
            "rows_loaded": 0,
            "status": "failed"
        }
    
    # Define task dependencies
    polls_data = extract_polls()
    load_result = load_polls(polls_data)
    
    # Success and failure logging
    log_ingestion_success(load_result)
    log_ingestion_failure()


# Instantiate the DAG
dawum_dag = dawum_ingest_dag()
