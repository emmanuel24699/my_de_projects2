from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/usr/local/airflow/dags/scripts')

from etl_functions import (
    load_metadata_to_rds,
    extract_metadata,
    extract_streaming,
    transform_kpis,
    validate_data,
    load_redshift
)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 13),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': ['admin@example.com'],
    'email_on_retry': False,
}

with DAG(
    'music_streaming_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    description='ETL pipeline for music streaming analytics',
) as dag:
    load_metadata_to_rds_task = PythonOperator(
        task_id='load_metadata_to_rds',
        python_callable=load_metadata_to_rds,
        execution_timeout=timedelta(minutes=30)
    )
    extract_metadata_task = PythonOperator(
        task_id='extract_metadata',
        python_callable=extract_metadata
    )
    extract_streaming_task = PythonOperator(
        task_id='extract_streaming',
        python_callable=extract_streaming
    )
    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data
    )
    transform_kpis_task = PythonOperator(
        task_id='transform_kpis',
        python_callable=transform_kpis
    )
    load_redshift_task = PythonOperator(
        task_id='load_redshift',
        python_callable=load_redshift
    )

    load_metadata_to_rds_task >> extract_metadata_task
    extract_metadata_task >> validate_data_task
    extract_streaming_task >> validate_data_task
    validate_data_task >> transform_kpis_task >> load_redshift_task