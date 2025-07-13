"""
Airflow DAG for loading data into ClickHouse.
Orchestrates data loading from various sources into the analytics warehouse.
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule

import sys
import os
import json
import pandas as pd
from pathlib import Path

sys.path.append('/opt/airflow')

from elt.loader_clickhouse import ClickHouseLoader, ClickHouseConfig
from elt.metadata import (
    create_raw_ingestion, DataSource, IngestionStatus,
    create_quality_check, TransformationJob
)


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
    'load_clickhouse_data',
    default_args=default_args,
    description='Load data into ClickHouse analytics warehouse',
    schedule_interval=timedelta(hours=1),  # Run every hour
    max_active_runs=1,
    tags=['loading', 'clickhouse', 'warehouse'],
)


@task
def check_source_data(**context) -> List[Dict[str, Any]]:
    """Check for new data files to load."""
    
    # Define source directories
    source_dirs = [
        {'path': '/tmp', 'pattern': 'dawum_data_*.json', 'source': 'dawum'},
        {'path': '/tmp', 'pattern': 'destatis_data_*.json', 'source': 'destatis'},
        {'path': '/tmp', 'pattern': 'eurostat_data_*.json', 'source': 'eurostat'},
        {'path': '/tmp', 'pattern': 'gesis_data_*.json', 'source': 'gesis'},
        {'path': '/tmp', 'pattern': 'soep_data_*.json', 'source': 'soep'},
        {'path': '/tmp', 'pattern': 'scraped_*.json', 'source': 'web_scraping'},
    ]
    
    available_files = []
    
    for source_dir in source_dirs:
        search_path = Path(source_dir['path'])
        
        if search_path.exists():
            # Find files matching pattern
            files = list(search_path.glob(source_dir['pattern']))
            
            for file_path in files:
                if file_path.is_file():
                    # Get file info
                    stat = file_path.stat()
                    
                    available_files.append({
                        'file_path': str(file_path),
                        'source': source_dir['source'],
                        'size': stat.st_size,
                        'modified': datetime.fromtimestamp(stat.st_mtime),
                        'dataset': file_path.stem.replace(f"{source_dir['source']}_data_", "")
                    })
    
    # Sort by modification time (newest first)
    available_files.sort(key=lambda x: x['modified'], reverse=True)
    
    context['task_instance'].xcom_push(key='available_files', value=available_files)
    
    return available_files


@task
def initialize_clickhouse_schema(**context) -> Dict[str, Any]:
    """Initialize ClickHouse database schema."""
    
    with ClickHouseLoader() as loader:
        # Create databases
        loader.create_database('raw')
        loader.create_database('staging')
        loader.create_database('analytics')
        
        # Create raw data tables
        
        # Raw ingestions tracking table
        loader.create_table(
            'raw.ingestions',
            '''
            (
                id UUID DEFAULT generateUUIDv4(),
                source String,
                dataset String,
                ingestion_time DateTime64(3),
                records_count UInt64,
                file_path String,
                status String,
                error_message String,
                metadata String
            )
            ENGINE = MergeTree()
            ORDER BY (source, dataset, ingestion_time)
            '''
        )
        
        # Raw DAWUM polls table
        loader.create_table(
            'raw.dawum_polls',
            '''
            (
                id UInt64,
                parliament_id UInt64,
                tasker_id UInt64,
                institute_id UInt64,
                method_id UInt64,
                date DateTime,
                survey_period_start Nullable(DateTime),
                survey_period_end Nullable(DateTime),
                results String,
                sample_size Nullable(UInt64),
                ingestion_time DateTime64(3) DEFAULT now(),
                raw_data String
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (id, date)
            '''
        )
        
        # Raw economic indicators table
        loader.create_table(
            'raw.economic_indicators',
            '''
            (
                id UUID DEFAULT generateUUIDv4(),
                source String,
                indicator_code String,
                indicator_name String,
                country_code String,
                region_code Nullable(String),
                value Float64,
                unit String,
                reference_date DateTime,
                collection_date DateTime64(3),
                ingestion_time DateTime64(3) DEFAULT now(),
                raw_data String
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (source, indicator_code, country_code, reference_date)
            '''
        )
        
        # Raw web scraping table
        loader.create_table(
            'raw.web_scraping',
            '''
            (
                id UUID DEFAULT generateUUIDv4(),
                source_url String,
                scraping_date DateTime64(3),
                page_title Nullable(String),
                content String,
                content_type String,
                spider_name String,
                ingestion_time DateTime64(3) DEFAULT now(),
                raw_data String
            )
            ENGINE = ReplacingMergeTree()
            ORDER BY (source_url, scraping_date)
            '''
        )
        
        # Data quality checks table
        loader.create_table(
            'analytics.data_quality_checks',
            '''
            (
                id UUID DEFAULT generateUUIDv4(),
                table_name String,
                check_name String,
                check_type String,
                passed UInt8,
                failed_rows UInt64,
                total_rows UInt64,
                error_message Nullable(String),
                check_date DateTime64(3),
                metadata String
            )
            ENGINE = MergeTree()
            ORDER BY (table_name, check_date)
            '''
        )
        
        return {
            'status': 'success',
            'message': 'ClickHouse schema initialized successfully'
        }


@task
def load_data_file(file_info: Dict[str, Any], **context) -> Dict[str, Any]:
    """Load a single data file into ClickHouse."""
    
    file_path = file_info['file_path']
    source = file_info['source']
    
    try:
        with ClickHouseLoader() as loader:
            # Load JSON data
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Ensure data is a list
            if not isinstance(data, list):
                data = [data]
            
            records_loaded = 0
            
            # Route data to appropriate table based on source
            if source == 'dawum':
                # Transform DAWUM data
                transformed_data = []
                for item in data:
                    if isinstance(item, dict) and 'id' in item:
                        transformed_item = {
                            'id': item.get('id'),
                            'parliament_id': item.get('parliament_id'),
                            'tasker_id': item.get('tasker_id'),
                            'institute_id': item.get('institute_id'),
                            'method_id': item.get('method_id'),
                            'date': item.get('date'),
                            'survey_period_start': item.get('survey_period_start'),
                            'survey_period_end': item.get('survey_period_end'),
                            'results': json.dumps(item.get('results', {})),
                            'sample_size': item.get('sample_size'),
                            'raw_data': json.dumps(item)
                        }
                        transformed_data.append(transformed_item)
                
                if transformed_data:
                    records_loaded = loader.insert_json_data('raw.dawum_polls', transformed_data)
            
            elif source in ['destatis', 'eurostat']:
                # Transform economic indicator data
                transformed_data = []
                for item in data:
                    if isinstance(item, dict):
                        transformed_item = {
                            'source': source,
                            'indicator_code': item.get('dataset', ''),
                            'indicator_name': item.get('title', ''),
                            'country_code': 'DE' if source == 'destatis' else 'EU',
                            'region_code': None,
                            'value': 0.0,  # TODO: Extract actual values
                            'unit': '',
                            'reference_date': datetime.now(),
                            'collection_date': datetime.now(),
                            'raw_data': json.dumps(item)
                        }
                        transformed_data.append(transformed_item)
                
                if transformed_data:
                    records_loaded = loader.insert_json_data('raw.economic_indicators', transformed_data)
            
            elif source == 'web_scraping':
                # Transform web scraping data
                transformed_data = []
                for item in data:
                    if isinstance(item, dict):
                        transformed_item = {
                            'source_url': item.get('url', ''),
                            'scraping_date': datetime.now(),
                            'page_title': item.get('title', ''),
                            'content': item.get('content', ''),
                            'content_type': 'html',
                            'spider_name': item.get('spider', ''),
                            'raw_data': json.dumps(item)
                        }
                        transformed_data.append(transformed_item)
                
                if transformed_data:
                    records_loaded = loader.insert_json_data('raw.web_scraping', transformed_data)
            
            else:
                # Generic loading for other sources
                records_loaded = loader.insert_json_data('raw.generic_data', data)
            
            # Log ingestion
            ingestion_record = create_raw_ingestion(
                source=DataSource(source),
                dataset=file_info['dataset'],
                records_count=records_loaded,
                file_path=file_path,
                status=IngestionStatus.SUCCESS
            )
            
            loader.log_ingestion(ingestion_record)
            
            return {
                'file_path': file_path,
                'source': source,
                'records_loaded': records_loaded,
                'status': 'success'
            }
            
    except Exception as e:
        # Log failed ingestion
        with ClickHouseLoader() as loader:
            ingestion_record = create_raw_ingestion(
                source=DataSource(source),
                dataset=file_info['dataset'],
                records_count=0,
                file_path=file_path,
                status=IngestionStatus.FAILED,
                error_message=str(e)
            )
            
            loader.log_ingestion(ingestion_record)
        
        return {
            'file_path': file_path,
            'source': source,
            'records_loaded': 0,
            'status': 'failed',
            'error': str(e)
        }


@task
def run_data_quality_checks(load_results: List[Dict[str, Any]], **context) -> Dict[str, Any]:
    """Run data quality checks on loaded data."""
    
    quality_checks = []
    
    with ClickHouseLoader() as loader:
        # Check for duplicate records
        duplicate_check = loader.execute_query('''
            SELECT 
                'raw.dawum_polls' as table_name,
                COUNT(*) as total_rows,
                COUNT(DISTINCT id) as unique_ids,
                COUNT(*) - COUNT(DISTINCT id) as duplicates
            FROM raw.dawum_polls
            WHERE toDate(ingestion_time) = toDate(now())
        ''')
        
        if duplicate_check:
            check_result = duplicate_check[0]
            quality_checks.append({
                'table_name': 'raw.dawum_polls',
                'check_name': 'duplicate_ids',
                'check_type': 'unique',
                'passed': check_result[3] == 0,  # duplicates == 0
                'failed_rows': check_result[3],
                'total_rows': check_result[1]
            })
        
        # Check for null values in critical fields
        null_check = loader.execute_query('''
            SELECT 
                'raw.dawum_polls' as table_name,
                COUNT(*) as total_rows,
                COUNT(CASE WHEN id IS NULL THEN 1 END) as null_ids,
                COUNT(CASE WHEN date IS NULL THEN 1 END) as null_dates
            FROM raw.dawum_polls
            WHERE toDate(ingestion_time) = toDate(now())
        ''')
        
        if null_check:
            check_result = null_check[0]
            quality_checks.append({
                'table_name': 'raw.dawum_polls',
                'check_name': 'null_critical_fields',
                'check_type': 'not_null',
                'passed': check_result[2] == 0 and check_result[3] == 0,
                'failed_rows': check_result[2] + check_result[3],
                'total_rows': check_result[1]
            })
        
        # Store quality check results
        for check in quality_checks:
            quality_record = create_quality_check(
                table_name=check['table_name'],
                check_name=check['check_name'],
                check_type=check['check_type'],
                passed=check['passed'],
                failed_rows=check['failed_rows'],
                total_rows=check['total_rows']
            )
            
            quality_data = {
                'table_name': quality_record.table_name,
                'check_name': quality_record.check_name,
                'check_type': quality_record.check_type,
                'passed': 1 if quality_record.passed else 0,
                'failed_rows': quality_record.failed_rows,
                'total_rows': quality_record.total_rows,
                'error_message': quality_record.error_message,
                'check_date': quality_record.check_date,
                'metadata': json.dumps(quality_record.metadata or {})
            }
            
            loader.insert_json_data('analytics.data_quality_checks', [quality_data])
    
    # Summary
    total_checks = len(quality_checks)
    passed_checks = sum(1 for check in quality_checks if check['passed'])
    
    return {
        'total_checks': total_checks,
        'passed_checks': passed_checks,
        'failed_checks': total_checks - passed_checks,
        'quality_checks': quality_checks,
        'status': 'success' if passed_checks == total_checks else 'warning'
    }


@task
def cleanup_processed_files(load_results: List[Dict[str, Any]], **context) -> None:
    """Clean up processed data files."""
    
    archive_dir = Path('/opt/airflow/data/raw/archive')
    archive_dir.mkdir(parents=True, exist_ok=True)
    
    for result in load_results:
        if result['status'] == 'success':
            file_path = Path(result['file_path'])
            
            if file_path.exists():
                # Move to archive
                archive_path = archive_dir / f"{file_path.stem}_{context['ds']}{file_path.suffix}"
                
                import shutil
                shutil.move(str(file_path), str(archive_path))
                
                print(f"Archived: {file_path} -> {archive_path}")


@task
def generate_load_report(
    load_results: List[Dict[str, Any]], 
    quality_results: Dict[str, Any], 
    **context
) -> Dict[str, Any]:
    """Generate loading report."""
    
    successful_loads = [r for r in load_results if r['status'] == 'success']
    failed_loads = [r for r in load_results if r['status'] == 'failed']
    
    total_records = sum(r['records_loaded'] for r in successful_loads)
    
    report = {
        'date': context['ds'],
        'total_files_processed': len(load_results),
        'successful_loads': len(successful_loads),
        'failed_loads': len(failed_loads),
        'total_records_loaded': total_records,
        'quality_checks': {
            'total_checks': quality_results['total_checks'],
            'passed_checks': quality_results['passed_checks'],
            'failed_checks': quality_results['failed_checks']
        },
        'sources': {}
    }
    
    # Group by source
    for result in load_results:
        source = result['source']
        if source not in report['sources']:
            report['sources'][source] = {'files': 0, 'records': 0, 'status': 'success'}
        
        report['sources'][source]['files'] += 1
        report['sources'][source]['records'] += result['records_loaded']
        
        if result['status'] == 'failed':
            report['sources'][source]['status'] = 'failed'
    
    # Save report
    report_file = f"/opt/airflow/data/reports/load_report_{context['ds']}.json"
    os.makedirs(os.path.dirname(report_file), exist_ok=True)
    
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    return report


# Define task dependencies
with dag:
    # Initialize schema
    init_schema = initialize_clickhouse_schema()
    
    # Check for source data
    check_data = check_source_data()
    
    # Load data files (dynamic task mapping)
    load_tasks = load_data_file.expand(
        file_info="{{ ti.xcom_pull(task_ids='check_source_data') }}"
    )
    
    # Run quality checks
    quality_checks = run_data_quality_checks(load_tasks)
    
    # Generate report
    report_task = generate_load_report(load_tasks, quality_checks)
    
    # Cleanup files
    cleanup_task = cleanup_processed_files(load_tasks)
    
    # Set dependencies
    init_schema >> check_data >> load_tasks >> [quality_checks, cleanup_task] >> report_task


if __name__ == "__main__":
    # Test the DAG
    dag.test()
