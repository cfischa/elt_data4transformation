import sys
sys.path.append('/opt/airflow')
"""
Airflow DAG for Destatis API data extraction only.
"""

from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.decorators import task
import os
import aiofiles
import orjson

from connectors.destatis_connector import DestatisConnector

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

dag = DAG(
    'destatis_extract_data',
    default_args=default_args,
    description='Extract data from Destatis API only',
    schedule_interval=timedelta(hours=6),
    max_active_runs=1,
    tags=['extraction', 'destatis'],
)

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
def validate_extracted_data(extraction_result: Dict[str, Any]) -> Dict[str, Any]:
    """Validate extracted data from Destatis."""
    validation_errors = []
    if extraction_result['status'] != 'success':
        validation_errors.append(f"Extraction failed for destatis")
    else:
        if extraction_result['records_count'] == 0:
            validation_errors.append("No data extracted from destatis")
        if not os.path.exists(extraction_result['output_file']):
            validation_errors.append("Output file missing for destatis")

    return {
        'total_records': extraction_result['records_count'],
        'validation_errors': validation_errors,
        'status': 'success' if not validation_errors else 'warning'
    }

@task
def send_notification(validation_result: Dict[str, Any], **context) -> None:
    """Send notification about extraction results."""
    from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
    webhook_token = os.getenv('SLACK_WEBHOOK_URL')
    if webhook_token:
        slack_hook = SlackWebhookHook(
            http_conn_id='slack_default',
            webhook_token=webhook_token
        )
        status_emoji = "✅" if validation_result['status'] == 'success' else "⚠️"
        message = f"""
        {status_emoji} *Destatis Data Extraction Complete*
        *Date:* {context['ds']}
        *Records Extracted:* {validation_result['total_records']}
        *Status:* {validation_result['status']}
        {('*Errors:*\\n' + '\\n'.join(validation_result['validation_errors'])) if validation_result['validation_errors'] else ''}
        """
        slack_hook.send_text(message)

with dag:
    destatis_task = extract_destatis_data()
    validation_task = validate_extracted_data(destatis_task)
    notification_task = send_notification(validation_task)
    destatis_task >> validation_task >> notification_task
