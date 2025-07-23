"""
Airflow DAG for web scraping jobs.
Orchestrates web scraping tasks using Scrapy spiders.
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule

import sys
import os
import subprocess
import json
from pathlib import Path

sys.path.append('/opt/airflow')

from elt.loader_clickhouse import ClickHouseLoader
from elt.metadata import create_raw_ingestion, DataSource, IngestionStatus


# Default DAG arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'catchup': False,
}

# DAG definition
dag = DAG(
    'extract_scrape_data',
    default_args=default_args,
    description='Extract data using web scraping',
    schedule_interval=timedelta(days=1),  # Run daily
    max_active_runs=1,
    tags=['extraction', 'scraping', 'web'],
)


@task
def check_scraping_targets(**context) -> List[Dict[str, Any]]:
    """Check which websites should be scraped today."""
    
    # TODO: Implement dynamic scraping target selection
    # - Check robots.txt compliance
    # - Verify website availability
    # - Check for recent updates
    # - Respect rate limits and scraping schedules
    
    scraping_targets = [
        {
            'spider': 'news_spider',
            'domain': 'news-website.com',
            'priority': 'high',
            'estimated_duration': 30,  # minutes
            'max_pages': 100,
            'enabled': True
        },
        {
            'spider': 'government_spider',
            'domain': 'government.gov',
            'priority': 'medium',
            'estimated_duration': 20,  # minutes
            'max_pages': 50,
            'enabled': True
        },
        {
            'spider': 'social_media_spider',
            'domain': 'social-platform.com',
            'priority': 'low',
            'estimated_duration': 45,  # minutes
            'max_pages': 200,
            'enabled': False  # Disabled for now
        }
    ]
    
    # Filter enabled targets
    enabled_targets = [target for target in scraping_targets if target['enabled']]
    
    context['task_instance'].xcom_push(key='scraping_targets', value=enabled_targets)
    
    return enabled_targets


@task
def run_scrapy_spider(spider_config: Dict[str, Any], **context) -> Dict[str, Any]:
    """Run a Scrapy spider."""
    
    spider_name = spider_config['spider']
    output_file = f"/tmp/scraped_{spider_name}_{context['ds']}.json"
    
    try:
        # Build Scrapy command
        cmd = [
            'scrapy', 'crawl', spider_name,
            '-o', output_file,
            '-s', f'CLOSESPIDER_PAGECOUNT={spider_config["max_pages"]}',
            '-s', 'JOBDIR=/tmp/scrapy_jobs',
            '-L', 'INFO'
        ]
        
        # Change to scraping directory
        cwd = '/opt/airflow/scraping'
        
        # Run spider
        result = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=spider_config['estimated_duration'] * 60  # Convert to seconds
        )
        
        if result.returncode == 0:
            # Check if output file was created
            if os.path.exists(output_file):
                # Count scraped items
                with open(output_file, 'r') as f:
                    try:
                        data = json.load(f)
                        item_count = len(data) if isinstance(data, list) else 1
                    except json.JSONDecodeError:
                        item_count = 0
                
                return {
                    'spider': spider_name,
                    'status': 'success',
                    'output_file': output_file,
                    'items_scraped': item_count,
                    'duration': spider_config['estimated_duration'],
                    'stdout': result.stdout,
                    'stderr': result.stderr
                }
            else:
                return {
                    'spider': spider_name,
                    'status': 'failed',
                    'error': 'Output file not created',
                    'stdout': result.stdout,
                    'stderr': result.stderr
                }
        else:
            return {
                'spider': spider_name,
                'status': 'failed',
                'error': f'Spider failed with return code {result.returncode}',
                'stdout': result.stdout,
                'stderr': result.stderr
            }
            
    except subprocess.TimeoutExpired:
        return {
            'spider': spider_name,
            'status': 'failed',
            'error': 'Spider timed out',
            'timeout': True
        }
    except Exception as e:
        return {
            'spider': spider_name,
            'status': 'failed',
            'error': str(e)
        }


@task
def validate_scraped_data(scraping_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Validate scraped data quality."""
    
    total_items = 0
    successful_spiders = 0
    failed_spiders = 0
    validation_errors = []
    
    for result in scraping_results:
        if result['status'] == 'success':
            successful_spiders += 1
            total_items += result.get('items_scraped', 0)
            
            # Check for minimum data quality
            if result.get('items_scraped', 0) == 0:
                validation_errors.append(f"No items scraped by {result['spider']}")
            
            # Check file size
            output_file = result.get('output_file')
            if output_file and os.path.exists(output_file):
                file_size = os.path.getsize(output_file)
                if file_size < 100:  # Less than 100 bytes
                    validation_errors.append(f"Output file too small for {result['spider']}")
            
        else:
            failed_spiders += 1
            validation_errors.append(f"Spider {result['spider']} failed: {result.get('error', 'Unknown error')}")
    
    return {
        'total_items': total_items,
        'successful_spiders': successful_spiders,
        'failed_spiders': failed_spiders,
        'validation_errors': validation_errors,
        'status': 'success' if failed_spiders == 0 else 'partial' if successful_spiders > 0 else 'failed'
    }


@task
def clean_scraped_data(scraping_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Clean and preprocess scraped data."""
    
    import re
    from datetime import datetime
    
    cleaned_files = []
    
    for result in scraping_results:
        if result['status'] == 'success' and result.get('output_file'):
            input_file = result['output_file']
            output_file = input_file.replace('.json', '_cleaned.json')
            
            try:
                with open(input_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                cleaned_data = []
                
                for item in data if isinstance(data, list) else [data]:
                    # Clean text fields
                    if 'title' in item and item['title']:
                        item['title'] = re.sub(r'\s+', ' ', item['title']).strip()
                    
                    if 'content' in item and item['content']:
                        # Remove excessive whitespace
                        item['content'] = re.sub(r'\s+', ' ', item['content']).strip()
                        # Remove HTML tags if present
                        item['content'] = re.sub(r'<[^>]+>', '', item['content'])
                    
                    # Add processing metadata
                    item['processed_at'] = datetime.now().isoformat()
                    item['processing_version'] = '1.0'
                    
                    cleaned_data.append(item)
                
                # Save cleaned data
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(cleaned_data, f, indent=2, ensure_ascii=False)
                
                cleaned_files.append({
                    'spider': result['spider'],
                    'original_file': input_file,
                    'cleaned_file': output_file,
                    'items_processed': len(cleaned_data)
                })
                
            except Exception as e:
                cleaned_files.append({
                    'spider': result['spider'],
                    'original_file': input_file,
                    'error': str(e)
                })
    
    return {
        'cleaned_files': cleaned_files,
        'status': 'success'
    }


@task
def archive_scraped_data(scraping_results: List[Dict[str, Any]], **context) -> None:
    """Archive scraped data to permanent storage."""
    
    archive_dir = Path('/opt/airflow/data/raw/scraped')
    archive_dir.mkdir(parents=True, exist_ok=True)
    
    for result in scraping_results:
        if result['status'] == 'success' and result.get('output_file'):
            source_file = Path(result['output_file'])
            
            if source_file.exists():
                # Create archive filename with timestamp
                archive_filename = f"{result['spider']}_{context['ds']}.json"
                archive_path = archive_dir / archive_filename
                
                # Copy file to archive
                import shutil
                shutil.copy2(source_file, archive_path)
                
                # Clean up temporary file
                source_file.unlink()


@task
def update_scraping_schedule(validation_result: Dict[str, Any]) -> None:
    """Update scraping schedule based on results."""
    
    # TODO: Implement adaptive scheduling
    # - Increase frequency for successful spiders
    # - Decrease frequency for failed spiders
    # - Adjust based on data volume and quality
    # - Consider website-specific rate limits
    
    schedule_updates = []
    
    # Log schedule updates
    print(f"Scraping schedule updates: {schedule_updates}")


# Define task dependencies
with dag:
    # Check scraping targets
    targets_task = check_scraping_targets()
    
    # Run spiders for each target
    scraping_tasks = []
    for i in range(3):  # Maximum 3 spiders
        spider_task = run_scrapy_spider.override(task_id=f'run_spider_{i}')(
            spider_config=f"{{{{ ti.xcom_pull(key='scraping_targets')[{i}] if len(ti.xcom_pull(key='scraping_targets', default=[])) > {i} else None }}}}"
        )
        scraping_tasks.append(spider_task)
    
    # Validate scraped data
    validation_task = validate_scraped_data(scraping_tasks)
    
    # Clean scraped data
    cleaning_task = clean_scraped_data(scraping_tasks)
    
    # Archive data
    archive_task = archive_scraped_data(scraping_tasks)
    
    # Update schedule
    schedule_task = update_scraping_schedule(validation_task)
    
    # Set dependencies
    targets_task >> scraping_tasks
    
    # Each scraping task feeds into validation and cleaning
    for scraping_task in scraping_tasks:
        scraping_task >> validation_task
        scraping_task >> cleaning_task
    
    # Both validation and cleaning must complete before archiving
    [validation_task, cleaning_task] >> archive_task
    archive_task >> schedule_task


# Alternative implementation using BashOperator
def scraping_dag_bash():
    """Alternative implementation using BashOperator for Scrapy commands."""
    
    # Run news spider
    news_spider = BashOperator(
        task_id='run_news_spider',
        bash_command='''
        cd /opt/airflow/scraping && \
        scrapy crawl news_spider \
        -o /tmp/news_data_{{ ds }}.json \
        -s CLOSESPIDER_PAGECOUNT=100 \
        -L INFO
        ''',
        dag=dag
    )
    
    # Run government spider
    gov_spider = BashOperator(
        task_id='run_government_spider',
        bash_command='''
        cd /opt/airflow/scraping && \
        scrapy crawl government_spider \
        -o /tmp/government_data_{{ ds }}.json \
        -s CLOSESPIDER_PAGECOUNT=50 \
        -L INFO
        ''',
        dag=dag
    )
    
    # Validate outputs
    validate_outputs = BashOperator(
        task_id='validate_scraping_outputs',
        bash_command='''
        echo "Validating scraped data..."
        if [ -f /tmp/news_data_{{ ds }}.json ]; then
            echo "News data found: $(wc -l < /tmp/news_data_{{ ds }}.json) lines"
        fi
        if [ -f /tmp/government_data_{{ ds }}.json ]; then
            echo "Government data found: $(wc -l < /tmp/government_data_{{ ds }}.json) lines"
        fi
        ''',
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag
    )
    
    # Set dependencies
    [news_spider, gov_spider] >> validate_outputs


if __name__ == "__main__":
    # Test the DAG
    dag.test()
