"""
Airflow DAG to orchestrate Silver layer processing.

Triggers Spark job to process Bronze JSON data into Silver Parquet.
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'github_archive_silver_processing',
    default_args=default_args,
    description='Process Bronze data into Silver layer using Spark',
    schedule_interval=None,  # Manually triggered after Bronze ingestion
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['silver', 'spark', 'transformation'],
) as dag:
    
    process_bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver_transform',
        application='/opt/spark-jobs/bronze_to_silver.py',
        conn_id='spark_default',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.executor.memory': '2g',
            'spark.driver.memory': '1g',
            'spark.executor.cores': '2',
        },
        application_args=[
            's3a://bronze/github_events/',
            's3a://silver/github_events/',
        ],
        jars='/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar',
        verbose=True,
    )
