import sys
sys.path.append('/opt/airflow')
"""
Airflow DAG for Destatis API metadata-first cube extraction.
Implements the official 2025 GENESIS-Online REST API pattern:
1. Metadata discovery via catalogue/cubes
2. Raw cube extraction via data/cubefile
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List

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
    description='Extract German statistical cube data using metadata from fetch_destatis_metadata DAG',
    schedule_interval=timedelta(hours=12),  # Twice daily
    max_active_runs=1,
    tags=['extraction', 'destatis', 'metadata-first', 'cubes'],
)

@task
def load_cube_metadata(**context) -> Dict[str, Any]:
    """Load cube metadata from the metadata DAG output."""
    import orjson
    
    metadata_file = "meta_data/destatis_cubes_metadata.json"
    
    if not os.path.exists(metadata_file):
        raise FileNotFoundError(f"Metadata file not found: {metadata_file}. Please run fetch_destatis_metadata DAG first.")
    
    with open(metadata_file, 'rb') as f:
        all_metadata = orjson.loads(f.read())
    
    # For now, just select first 10 cubes for testing
    # Later you can modify this to select specific cubes manually
    selected_cubes = all_metadata[:10]
    
    return {
        'total_cubes_available': len(all_metadata),
        'selected_cubes_count': len(selected_cubes),
        'selected_cubes': [cube['code'] for cube in selected_cubes],
        'metadata_file': metadata_file,
        'note': 'Currently selecting first 10 cubes for testing. Modify selection logic as needed.',
        'status': 'success'
    }

@task
def extract_destatis_cubes(metadata_result: Dict[str, Any], **context) -> Dict[str, Any]:
    """Extract cube data from Destatis using pre-loaded metadata."""
    import asyncio

    async def run_extraction():
        async with DestatisConnector() as connector:
            data_count = 0
            extracted_cubes = []

            # Extract each selected cube
            for cube_name in metadata_result['selected_cubes'][:5]:  # Limit to first 5 cubes
                try:
                    cube_data = await connector.fetch_cube(cube_name)
                    if cube_data:
                        extracted_cubes.append({
                            'cube_name': cube_name,
                            'data': cube_data,
                            'extracted_at': context['ds']
                        })
                        data_count += 1
                except Exception as e:
                    print(f"Failed to extract cube {cube_name}: {str(e)}")
                    continue

            # Save extracted cube data
            output_file = f"/tmp/destatis_cubes_{context['ds']}.json"
            async with aiofiles.open(output_file, 'w') as f:
                await f.write(orjson.dumps(extracted_cubes, option=orjson.OPT_INDENT_2).decode())

            return {
                'source': 'destatis',
                'cubes_extracted': data_count,
                'output_file': output_file,
                'metadata_file': metadata_result['metadata_file'],
                'status': 'success'
            }

    return asyncio.run(run_extraction())

@task
def validate_extracted_cubes(extraction_result: Dict[str, Any]) -> Dict[str, Any]:
    """Validate extracted cube data from Destatis."""
    validation_errors = []
    if extraction_result['status'] != 'success':
        validation_errors.append("Extraction failed for destatis cubes")
    else:
        if extraction_result['cubes_extracted'] == 0:
            validation_errors.append("No cubes extracted from destatis")
        if not os.path.exists(extraction_result['output_file']):
            validation_errors.append("Cube output file missing for destatis")
        if not os.path.exists(extraction_result['metadata_file']):
            validation_errors.append("Metadata file missing for destatis")

    return {
        'cubes_extracted': extraction_result['cubes_extracted'],
        'validation_errors': validation_errors,
        'status': 'success' if not validation_errors else 'warning'
    }

@task
def send_notification(validation_result: Dict[str, Any], **context) -> None:
    """Send notification about cube extraction results."""
    from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
    webhook_token = os.getenv('SLACK_WEBHOOK_URL')
    if webhook_token:
        slack_hook = SlackWebhookHook(
            http_conn_id='slack_default',
            webhook_token=webhook_token
        )
        status_emoji = "✅" if validation_result['status'] == 'success' else "⚠️"
        errors = validation_result['validation_errors']
        error_block = ""
        if errors:
            error_block = "*Errors:*\n" + "\n".join(errors)

        message = (
            f"{status_emoji} *Destatis Cube Extraction Complete*\n"
            f"*Date:* {context['ds']}\n"
            f"*Cubes Extracted:* {validation_result['cubes_extracted']}\n"
            f"*Status:* {validation_result['status']}\n"
            f"{error_block}"
        )
        slack_hook.send_text(message)

with dag:
    # Load metadata from separate metadata DAG
    metadata_task = load_cube_metadata()
    extraction_task = extract_destatis_cubes(metadata_task)
    validation_task = validate_extracted_cubes(extraction_task)
    notification_task = send_notification(validation_task)
    
    # Define task dependencies
    metadata_task >> extraction_task >> validation_task >> notification_task
