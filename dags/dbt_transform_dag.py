"""
Airflow DAG for dbt transformations.
Orchestrates dbt models, tests, and documentation generation.
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
import json
import subprocess
from pathlib import Path

sys.path.append('/opt/airflow')

from elt.loader_clickhouse import ClickHouseLoader


# Default DAG arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# DAG definition
dag = DAG(
    'dbt_transform_data',
    default_args=default_args,
    description='Run dbt transformations on ClickHouse data',
    schedule_interval=timedelta(hours=2),  # Run every 2 hours
    max_active_runs=1,
    tags=['transformation', 'dbt', 'analytics'],
)


@task
def check_dbt_environment(**context) -> Dict[str, Any]:
    """Check dbt environment and dependencies."""
    
    try:
        # Check dbt version
        result = subprocess.run(
            ['dbt', '--version'],
            capture_output=True,
            text=True,
            cwd='/usr/app/dbt_project'
        )
        
        if result.returncode != 0:
            raise RuntimeError(f"dbt not available: {result.stderr}")
        
        # Check dbt profiles
        profiles_result = subprocess.run(
            ['dbt', 'debug', '--profiles-dir', '/root/.dbt'],
            capture_output=True,
            text=True,
            cwd='/usr/app/dbt_project'
        )
        
        return {
            'dbt_version': result.stdout.strip(),
            'profiles_ok': profiles_result.returncode == 0,
            'profiles_output': profiles_result.stdout,
            'status': 'success'
        }
        
    except Exception as e:
        return {
            'status': 'failed',
            'error': str(e)
        }


@task
def check_source_data(**context) -> Dict[str, Any]:
    """Check if source data is available for transformation."""
    
    with ClickHouseLoader() as loader:
        # Check raw data availability
        data_checks = {}
        
        # Check DAWUM polls
        dawum_count = loader.execute_query('''
            SELECT COUNT(*) as count 
            FROM raw.dawum_polls 
            WHERE toDate(ingestion_time) >= toDate(now()) - INTERVAL 1 DAY
        ''')
        data_checks['dawum_polls'] = dawum_count[0][0] if dawum_count else 0
        
        # Check economic indicators
        economic_count = loader.execute_query('''
            SELECT COUNT(*) as count 
            FROM raw.economic_indicators 
            WHERE toDate(ingestion_time) >= toDate(now()) - INTERVAL 1 DAY
        ''')
        data_checks['economic_indicators'] = economic_count[0][0] if economic_count else 0
        
        # Check web scraping data
        scraping_count = loader.execute_query('''
            SELECT COUNT(*) as count 
            FROM raw.web_scraping 
            WHERE toDate(ingestion_time) >= toDate(now()) - INTERVAL 1 DAY
        ''')
        data_checks['web_scraping'] = scraping_count[0][0] if scraping_count else 0
        
        total_records = sum(data_checks.values())
        
        return {
            'data_checks': data_checks,
            'total_records': total_records,
            'has_data': total_records > 0,
            'status': 'success'
        }


@task
def run_dbt_deps(**context) -> Dict[str, Any]:
    """Install dbt dependencies."""
    
    try:
        result = subprocess.run(
            ['dbt', 'deps', '--profiles-dir', '/root/.dbt'],
            capture_output=True,
            text=True,
            cwd='/usr/app/dbt_project'
        )
        
        return {
            'returncode': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'status': 'success' if result.returncode == 0 else 'failed'
        }
        
    except Exception as e:
        return {
            'status': 'failed',
            'error': str(e)
        }


@task
def run_dbt_seed(**context) -> Dict[str, Any]:
    """Run dbt seed to load reference data."""
    
    try:
        result = subprocess.run(
            ['dbt', 'seed', '--profiles-dir', '/root/.dbt'],
            capture_output=True,
            text=True,
            cwd='/usr/app/dbt_project'
        )
        
        return {
            'returncode': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'status': 'success' if result.returncode == 0 else 'failed'
        }
        
    except Exception as e:
        return {
            'status': 'failed',
            'error': str(e)
        }


@task
def run_dbt_raw_models(**context) -> Dict[str, Any]:
    """Run dbt raw models."""
    
    try:
        result = subprocess.run(
            ['dbt', 'run', '--models', 'raw', '--profiles-dir', '/root/.dbt'],
            capture_output=True,
            text=True,
            cwd='/usr/app/dbt_project'
        )
        
        return {
            'returncode': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'models_run': result.stdout.count('Completed successfully') if result.returncode == 0 else 0,
            'status': 'success' if result.returncode == 0 else 'failed'
        }
        
    except Exception as e:
        return {
            'status': 'failed',
            'error': str(e)
        }


@task
def run_dbt_staging_models(**context) -> Dict[str, Any]:
    """Run dbt staging models."""
    
    try:
        result = subprocess.run(
            ['dbt', 'run', '--models', 'staging', '--profiles-dir', '/root/.dbt'],
            capture_output=True,
            text=True,
            cwd='/usr/app/dbt_project'
        )
        
        return {
            'returncode': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'models_run': result.stdout.count('Completed successfully') if result.returncode == 0 else 0,
            'status': 'success' if result.returncode == 0 else 'failed'
        }
        
    except Exception as e:
        return {
            'status': 'failed',
            'error': str(e)
        }


@task
def run_dbt_marts_models(**context) -> Dict[str, Any]:
    """Run dbt marts models."""
    
    try:
        result = subprocess.run(
            ['dbt', 'run', '--models', 'marts', '--profiles-dir', '/root/.dbt'],
            capture_output=True,
            text=True,
            cwd='/usr/app/dbt_project'
        )
        
        return {
            'returncode': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'models_run': result.stdout.count('Completed successfully') if result.returncode == 0 else 0,
            'status': 'success' if result.returncode == 0 else 'failed'
        }
        
    except Exception as e:
        return {
            'status': 'failed',
            'error': str(e)
        }


@task
def run_dbt_tests(**context) -> Dict[str, Any]:
    """Run dbt tests."""
    
    try:
        result = subprocess.run(
            ['dbt', 'test', '--profiles-dir', '/root/.dbt'],
            capture_output=True,
            text=True,
            cwd='/usr/app/dbt_project'
        )
        
        # Parse test results
        test_results = {
            'total_tests': 0,
            'passed_tests': 0,
            'failed_tests': 0,
            'warnings': 0
        }
        
        if result.returncode == 0:
            # Parse successful output
            lines = result.stdout.split('\n')
            for line in lines:
                if 'Finished running' in line and 'test' in line:
                    # Extract test counts from summary line
                    import re
                    match = re.search(r'(\d+) test', line)
                    if match:
                        test_results['total_tests'] = int(match.group(1))
                        test_results['passed_tests'] = int(match.group(1))
        else:
            # Parse failed output
            lines = result.stderr.split('\n')
            for line in lines:
                if 'FAIL' in line:
                    test_results['failed_tests'] += 1
                elif 'PASS' in line:
                    test_results['passed_tests'] += 1
                elif 'WARN' in line:
                    test_results['warnings'] += 1
        
        test_results['total_tests'] = test_results['passed_tests'] + test_results['failed_tests']
        
        return {
            'returncode': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'test_results': test_results,
            'status': 'success' if result.returncode == 0 else 'failed'
        }
        
    except Exception as e:
        return {
            'status': 'failed',
            'error': str(e)
        }


@task
def run_dbt_docs_generate(**context) -> Dict[str, Any]:
    """Generate dbt documentation."""
    
    try:
        result = subprocess.run(
            ['dbt', 'docs', 'generate', '--profiles-dir', '/root/.dbt'],
            capture_output=True,
            text=True,
            cwd='/usr/app/dbt_project'
        )
        
        return {
            'returncode': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'status': 'success' if result.returncode == 0 else 'failed'
        }
        
    except Exception as e:
        return {
            'status': 'failed',
            'error': str(e)
        }


@task
def log_dbt_artifacts(
    raw_result: Dict[str, Any],
    staging_result: Dict[str, Any],
    marts_result: Dict[str, Any],
    test_result: Dict[str, Any],
    **context
) -> Dict[str, Any]:
    """Log dbt run artifacts to ClickHouse."""
    
    try:
        with ClickHouseLoader() as loader:
            # Create dbt artifacts table if it doesn't exist
            loader.create_table(
                'analytics.dbt_artifacts',
                '''
                (
                    run_id UUID DEFAULT generateUUIDv4(),
                    run_date DateTime64(3),
                    dag_run_id String,
                    layer String,
                    models_run UInt64,
                    models_failed UInt64,
                    duration_seconds Float64,
                    status String,
                    stdout String,
                    stderr String,
                    metadata String
                )
                ENGINE = MergeTree()
                ORDER BY (run_date, layer)
                '''
            )
            
            # Log artifacts for each layer
            artifacts = []
            
            layers = [
                ('raw', raw_result),
                ('staging', staging_result),
                ('marts', marts_result),
                ('tests', test_result)
            ]
            
            for layer_name, result in layers:
                if result['status'] != 'failed':
                    artifact = {
                        'run_date': datetime.now(),
                        'dag_run_id': context['dag_run'].run_id,
                        'layer': layer_name,
                        'models_run': result.get('models_run', 0),
                        'models_failed': 1 if result['status'] == 'failed' else 0,
                        'duration_seconds': 0.0,  # TODO: Calculate from logs
                        'status': result['status'],
                        'stdout': result.get('stdout', ''),
                        'stderr': result.get('stderr', ''),
                        'metadata': json.dumps(result)
                    }
                    artifacts.append(artifact)
            
            # Insert artifacts
            if artifacts:
                loader.insert_json_data('analytics.dbt_artifacts', artifacts)
            
            return {
                'artifacts_logged': len(artifacts),
                'status': 'success'
            }
            
    except Exception as e:
        return {
            'status': 'failed',
            'error': str(e)
        }


@task
def generate_transformation_report(
    raw_result: Dict[str, Any],
    staging_result: Dict[str, Any],
    marts_result: Dict[str, Any],
    test_result: Dict[str, Any],
    **context
) -> Dict[str, Any]:
    """Generate transformation report."""
    
    report = {
        'date': context['ds'],
        'dag_run_id': context['dag_run'].run_id,
        'layers': {
            'raw': {
                'status': raw_result['status'],
                'models_run': raw_result.get('models_run', 0)
            },
            'staging': {
                'status': staging_result['status'],
                'models_run': staging_result.get('models_run', 0)
            },
            'marts': {
                'status': marts_result['status'],
                'models_run': marts_result.get('models_run', 0)
            }
        },
        'tests': {
            'status': test_result['status'],
            'total_tests': test_result.get('test_results', {}).get('total_tests', 0),
            'passed_tests': test_result.get('test_results', {}).get('passed_tests', 0),
            'failed_tests': test_result.get('test_results', {}).get('failed_tests', 0),
        },
        'overall_status': 'success' if all(
            result['status'] == 'success' 
            for result in [raw_result, staging_result, marts_result, test_result]
        ) else 'failed'
    }
    
    # Save report
    report_file = f"/usr/app/data/reports/dbt_report_{context['ds']}.json"
    os.makedirs(os.path.dirname(report_file), exist_ok=True)
    
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    return report


# Define task dependencies using traditional approach
with dag:
    # Environment checks
    check_env = check_dbt_environment()
    check_data = check_source_data()
    
    # dbt setup
    dbt_deps = run_dbt_deps()
    dbt_seed = run_dbt_seed()
    
    # dbt transformations
    dbt_raw = run_dbt_raw_models()
    dbt_staging = run_dbt_staging_models()
    dbt_marts = run_dbt_marts_models()
    
    # dbt tests
    dbt_test = run_dbt_tests()
    
    # Documentation
    dbt_docs = run_dbt_docs_generate()
    
    # Logging and reporting
    log_artifacts = log_dbt_artifacts(dbt_raw, dbt_staging, dbt_marts, dbt_test)
    generate_report = generate_transformation_report(dbt_raw, dbt_staging, dbt_marts, dbt_test)
    
    # Set dependencies
    [check_env, check_data] >> dbt_deps >> dbt_seed >> dbt_raw >> dbt_staging >> dbt_marts >> dbt_test >> [dbt_docs, log_artifacts, generate_report]


# Alternative implementation using BashOperator
def dbt_dag_bash():
    """Alternative implementation using BashOperator."""
    
    # Run dbt deps
    dbt_deps_bash = BashOperator(
        task_id='dbt_deps',
        bash_command='cd /usr/app/dbt_project && dbt deps --profiles-dir /root/.dbt',
        dag=dag
    )
    
    # Run dbt seed
    dbt_seed_bash = BashOperator(
        task_id='dbt_seed',
        bash_command='cd /usr/app/dbt_project && dbt seed --profiles-dir /root/.dbt',
        dag=dag
    )
    
    # Run dbt models
    dbt_run_bash = BashOperator(
        task_id='dbt_run',
        bash_command='cd /usr/app/dbt_project && dbt run --profiles-dir /root/.dbt',
        dag=dag
    )
    
    # Run dbt tests
    dbt_test_bash = BashOperator(
        task_id='dbt_test',
        bash_command='cd /usr/app/dbt_project && dbt test --profiles-dir /root/.dbt',
        dag=dag
    )
    
    # Generate docs
    dbt_docs_bash = BashOperator(
        task_id='dbt_docs',
        bash_command='cd /usr/app/dbt_project && dbt docs generate --profiles-dir /root/.dbt',
        dag=dag
    )
    
    # Set dependencies
    dbt_deps_bash >> dbt_seed_bash >> dbt_run_bash >> dbt_test_bash >> dbt_docs_bash


if __name__ == "__main__":
    # Test the DAG
    dag.test()
