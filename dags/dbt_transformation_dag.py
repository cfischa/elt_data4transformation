"""
DBT Transformation DAG for BNB Data Pipeline

This DAG orchestrates dbt transformations after data ingestion.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy import DummyOperator

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG Definition
dag = DAG(
    'dbt_transformation_pipeline',
    default_args=default_args,
    description='DBT transformation pipeline for political polling data',
    schedule_interval='@daily',
    max_active_runs=1,
    catchup=False,
    tags=['dbt', 'transformation', 'data-pipeline'],
)

# Start task
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# File sensor to check if raw data is available
data_sensor = FileSensor(
    task_id='wait_for_raw_data',
    filepath='/opt/airflow/data/dawum/all_polls.json',
    fs_conn_id='fs_default',
    poke_interval=60,
    timeout=300,
    mode='poke',
    dag=dag,
)

# DBT debug to check connections
dbt_debug = BashOperator(
    task_id='dbt_debug',
    bash_command='docker exec dbt dbt debug --profiles-dir /usr/app/dbt_project',
    dag=dag,
)

# DBT deps to install dependencies
dbt_deps = BashOperator(
    task_id='dbt_deps',
    bash_command='docker exec dbt dbt deps --profiles-dir /usr/app/dbt_project',
    dag=dag,
)

# DBT seed to load reference data
dbt_seed = BashOperator(
    task_id='dbt_seed',
    bash_command='docker exec dbt dbt seed --profiles-dir /usr/app/dbt_project',
    dag=dag,
)

# DBT run to execute transformations
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='docker exec dbt dbt run --profiles-dir /usr/app/dbt_project',
    dag=dag,
)

# DBT test to validate data quality
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='docker exec dbt dbt test --profiles-dir /usr/app/dbt_project',
    dag=dag,
)

# DBT docs generate
dbt_docs = BashOperator(
    task_id='dbt_docs_generate',
    bash_command='docker exec dbt dbt docs generate --profiles-dir /usr/app/dbt_project',
    dag=dag,
)

# End task
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Task dependencies
start_task >> data_sensor >> dbt_debug >> dbt_deps >> dbt_seed >> dbt_run >> dbt_test >> dbt_docs >> end_task
