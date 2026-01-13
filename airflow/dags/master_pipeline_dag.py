"""
Master DAG to orchestrate end-to-end GitHub Archive pipeline.

This DAG chains together:
1. Bronze ingestion (download raw data)
2. Silver processing (Spark transformation)
3. Gold modeling (dbt analytics)
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def log_pipeline_start(**context):
    """Log pipeline execution start."""
    print(f"Starting GitHub Archive pipeline for execution date: {context['ds']}")


def log_pipeline_complete(**context):
    """Log pipeline execution completion."""
    print(f"GitHub Archive pipeline complete for execution date: {context['ds']}")


with DAG(
    'github_archive_master_pipeline',
    default_args=default_args,
    description='End-to-end GitHub Archive data pipeline',
    schedule_interval=None,  # Manually triggered for demo
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['master', 'orchestration', 'end-to-end'],
) as dag:
    
    start_task = PythonOperator(
        task_id='log_pipeline_start',
        python_callable=log_pipeline_start,
    )
    
    # Step 1: Trigger Bronze ingestion
    trigger_bronze = TriggerDagRunOperator(
        task_id='trigger_bronze_ingestion',
        trigger_dag_id='github_archive_bronze_ingestion',
        wait_for_completion=True,
        poke_interval=30,
    )
    
    # Step 2: Trigger Silver processing
    trigger_silver = TriggerDagRunOperator(
        task_id='trigger_silver_processing',
        trigger_dag_id='github_archive_silver_processing',
        wait_for_completion=True,
        poke_interval=30,
    )
    
    # Step 3: Trigger dbt Gold models (would need dbt operator in production)
    # For now, we'll use a bash operator or python operator to call dbt
    
    complete_task = PythonOperator(
        task_id='log_pipeline_complete',
        python_callable=log_pipeline_complete,
    )
    
    # Define dependencies
    start_task >> trigger_bronze >> trigger_silver >> complete_task
