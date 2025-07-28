"""
Airflow DAG for API data extraction.
Orchestrates data extraction from various APIs (DAWUM, Destatis, Eurostat, etc.).
"""

from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.decorators import task, dag
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

import sys
import os
import aiofiles
import orjson
sys.path.append('/opt/airflow')

from connectors.dawum_connector import DawumConnector
from connectors.destatis_connector import DestatisConnector
from connectors.eurostat_connector import EurostatConnector
from connectors.gesis_connector import GESISConnector
from connectors.soep_connector import SOEPConnector
from elt.loader_clickhouse import ClickHouseLoader
from elt.metadata import create_raw_ingestion, DataSource, IngestionStatus


# Default DAG arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# DAG definition
dag = DAG(
    'extract_api_data',
    default_args=default_args,
    description='Extract data from various APIs',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    max_active_runs=1,
    tags=['extraction', 'api', 'polling'],
)


@task
def extract_dawum_data(**context) -> Dict[str, Any]:
       #Extract data from DAWUM API.
    import asyncio
    
    async def run_extraction():
        async with DawumConnector() as connector:
            data_count = 0
            extracted_data = []
            
            # Fetch recent polls
            async for poll_data in connector.fetch_data():
                extracted_data.append(poll_data)
                data_count += 1
            
            # Save to file
            output_file = f"/tmp/dawum_data_{context['ds']}.json"
            async with aiofiles.open(output_file, 'w') as f:
                await f.write(orjson.dumps(extracted_data, option=orjson.OPT_INDENT_2).decode())
            
            return {
                'source': 'dawum',
                'records_count': data_count,
                'output_file': output_file,
                'status': 'success'
            }
    
    return asyncio.run(run_extraction())


@task
def extract_destatis_data(**context) -> Dict[str, Any]:
    """Extract data from Destatis API."""
    import asyncio
    
    async def run_extraction():
        async with DestatisConnector() as connector:
            data_count = 0
            extracted_data = []
            
            # Fetch available tables
            tables = await connector.get_available_tables()
            
            # Extract data for each table
            for table in tables[:5]:  # Limit to first 5 tables
                async for data in connector.fetch_data(table_ids=[table.name]):
                    extracted_data.append(data)
                    data_count += 1
            
            # Save to file
            output_file = f"/tmp/destatis_data_{context['ds']}.json"
            async with aiofiles.open(output_file, 'w') as f:
                await f.write(orjson.dumps(extracted_data, option=orjson.OPT_INDENT_2).decode())
            
            return {
                'source': 'destatis',
                'records_count': data_count,
                'output_file': output_file,
                'status': 'success'
            }
    
    return asyncio.run(run_extraction())


@task
def extract_eurostat_data(**context) -> Dict[str, Any]:
    #Extract data from Eurostat API.""
    import asyncio
    
    async def run_extraction():
        async with EurostatConnector() as connector:
            data_count = 0
            extracted_data = []
            
            # Fetch available datasets
            datasets = await connector.get_available_datasets()
            
            # Extract data for each dataset
            for dataset in datasets[:3]:  # Limit to first 3 datasets
                async for data in connector.fetch_data(datasets=[dataset['id']]):
                    extracted_data.append(data)
                    data_count += 1
            
            # Save to file
            output_file = f"/tmp/eurostat_data_{context['ds']}.json"
            async with aiofiles.open(output_file, 'w') as f:
                await f.write(orjson.dumps(extracted_data, option=orjson.OPT_INDENT_2).decode())
            
            return {
                'source': 'eurostat',
                'records_count': data_count,
                'output_file': output_file,
                'status': 'success'
            }
    
    return asyncio.run(run_extraction())


@task
def extract_gesis_data(**context) -> Dict[str, Any]:
    """Extract data from GESIS API."""
    import asyncio
    
    async def run_extraction():
        async with GESISConnector() as connector:
            data_count = 0
            extracted_data = []
            
            # Fetch available datasets
            datasets = await connector.get_available_datasets()
            
            # Extract data for each dataset
            for dataset in datasets[:2]:  # Limit to first 2 datasets
                async for data in connector.fetch_data(datasets=[dataset['id']]):
                    extracted_data.append(data)
                    data_count += 1
            
            # Save to file
            output_file = f"/tmp/gesis_data_{context['ds']}.json"
            async with aiofiles.open(output_file, 'w') as f:
                await f.write(orjson.dumps(extracted_data, option=orjson.OPT_INDENT_2).decode())
            
            return {
                'source': 'gesis',
                'records_count': data_count,
                'output_file': output_file,
                'status': 'success'
            }
    
    return asyncio.run(run_extraction())


@task
def extract_soep_data(**context) -> Dict[str, Any]:
    """Extract data from SOEP API."""
    import asyncio
    
    async def run_extraction():
        async with SOEPConnector() as connector:
            data_count = 0
            extracted_data = []
            
            # Fetch available datasets
            datasets = await connector.get_available_datasets()
            
            # Extract data for each dataset
            for dataset in datasets[:2]:  # Limit to first 2 datasets
                async for data in connector.fetch_data(datasets=[dataset['id']]):
                    extracted_data.append(data)
                    data_count += 1
            
            # Save to file
            output_file = f"/tmp/soep_data_{context['ds']}.json"
            async with aiofiles.open(output_file, 'w') as f:
                await f.write(orjson.dumps(extracted_data, option=orjson.OPT_INDENT_2).decode())
            
            return {
                'source': 'soep',
                'records_count': data_count,
                'output_file': output_file,
                'status': 'success'
            }
    
    return asyncio.run(run_extraction())


@task
def validate_extracted_data(extraction_results: list) -> Dict[str, Any]:
    """Validate extracted data from all sources."""
    total_records = 0
    sources_processed = 0
    validation_errors = []
    
    for result in extraction_results:
        if result['status'] == 'success':
            total_records += result['records_count']
            sources_processed += 1
            
            # Basic validation
            if result['records_count'] == 0:
                validation_errors.append(f"No data extracted from {result['source']}")
            
            # Check if file exists
            if not os.path.exists(result['output_file']):
                validation_errors.append(f"Output file missing for {result['source']}")
        else:
            validation_errors.append(f"Extraction failed for {result['source']}")
    
    return {
        'total_records': total_records,
        'sources_processed': sources_processed,
        'validation_errors': validation_errors,
        'status': 'success' if not validation_errors else 'warning'
    }


@task
def send_notification(validation_result: Dict[str, Any], **context) -> None:
    """Send notification about extraction results."""
    from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
    
    # Slack webhook configuration - set SLACK_WEBHOOK_URL environment variable
    webhook_token = os.getenv('SLACK_WEBHOOK_URL')
    
    if webhook_token:
        slack_hook = SlackWebhookHook(
            http_conn_id='slack_default',
            webhook_token=webhook_token
        )
        
        # Prepare message
        status_emoji = "✅" if validation_result['status'] == 'success' else "⚠️"
        message = f"""
        {status_emoji} *API Data Extraction Complete*
        
        *Date:* {context['ds']}
        *Records Extracted:* {validation_result['total_records']}
        *Sources Processed:* {validation_result['sources_processed']}
        *Status:* {validation_result['status']}
        
        {("*Errors:*\\n" + '\\n'.join(validation_result['validation_errors'])) if validation_result['validation_errors'] else ''}
        """
        
        slack_hook.send_text(message)


# Define task dependencies
with dag:
    # Extract data from all APIs in parallel
    dawum_task = extract_dawum_data()
    destatis_task = extract_destatis_data()
    eurostat_task = extract_eurostat_data()
    gesis_task = extract_gesis_data()
    soep_task = extract_soep_data()
    
    # Validate all extracted data
    validation_task = validate_extracted_data([
        dawum_task, destatis_task, eurostat_task, gesis_task, soep_task
    ])
    
    # Send notification
    notification_task = send_notification(validation_task)
    
    # Set dependencies
    [dawum_task, destatis_task, eurostat_task, gesis_task, soep_task] >> validation_task >> notification_task


# Alternative approach using traditional operators
def extract_api_data_traditional():
    """Traditional approach using PythonOperator."""
    
    def extract_dawum():
        import asyncio
        from connectors.dawum_connector import DawumConnector
        
        async def run():
            async with DawumConnector() as connector:
                data = []
                async for poll_data in connector.fetch_data():
                    data.append(poll_data)
                return data
        
        return asyncio.run(run())
    
    def extract_destatis():
        import asyncio
        from connectors.destatis_connector import DestatisConnector
        
        async def run():
            async with DestatisConnector() as connector:
                data = []
                async for dataset_data in connector.fetch_data():
                    data.append(dataset_data)
                return data
        
        return asyncio.run(run())
    
    # Define tasks
    dawum_extraction = PythonOperator(
        task_id='extract_dawum',
        python_callable=extract_dawum,
        dag=dag
    )
    
    destatis_extraction = PythonOperator(
        task_id='extract_destatis',
        python_callable=extract_destatis,
        dag=dag
    )
    
    # Additional extraction tasks can be added here as needed
    # Example: eurostat_extraction = PythonOperator(...)
    
    # Set dependencies
    dawum_extraction >> destatis_extraction


if __name__ == "__main__":
    # Test the DAG
    dag.test()
