"""
Airflow DAG for ingesting GitHub Archive data into Bronze layer.

This DAG downloads hourly GitHub Archive files for a configurable date range
and uploads them to MinIO (S3-compatible storage) with partitioning.
"""

from datetime import datetime, timedelta
import os
import tempfile
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

import sys
sys.path.insert(0, os.path.dirname(__file__))

from utils.github_archive_utils import (
    GitHubArchiveDownloader,
    MinIOUploader,
    generate_s3_path,
    generate_date_range
)


# Default arguments
default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def download_and_upload_hour(date_str: str, hour: int, **context):
    """
    Download a specific hour of GitHub Archive data and upload to MinIO.
    
    Args:
        date_str: Date in YYYY-MM-DD format
        hour: Hour (0-23)
    """
    date = datetime.strptime(date_str, "%Y-%m-%d")
    
    # Get configuration from environment or Airflow Variables
    minio_endpoint = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
    minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
    bucket_name = os.getenv('MINIO_BUCKET_BRONZE', 'bronze')
    
    # Initialize downloaders
    downloader = GitHubArchiveDownloader()
    uploader = MinIOUploader(minio_endpoint, minio_access_key, minio_secret_key)
    
    # Ensure bucket exists
    uploader.create_bucket_if_not_exists(bucket_name)
    
    # Generate S3 path
    bucket, s3_key = generate_s3_path(date, hour, bucket_name)
    
    # Check if already exists (idempotency)
    if uploader.file_exists(bucket, s3_key):
        print(f"File already exists at s3://{bucket}/{s3_key}, skipping")
        return
    
    # Download to temporary file
    with tempfile.NamedTemporaryFile(mode='wb', suffix='.json.gz', delete=False) as tmp_file:
        tmp_path = tmp_file.name
        
    try:
        # Download
        success = downloader.download_archive(date, hour, tmp_path)
        if not success:
            raise Exception(f"Failed to download data for {date_str} hour {hour}")
        
        # Validate
        if not downloader.validate_gzip(tmp_path):
            raise Exception(f"Downloaded file is invalid for {date_str} hour {hour}")
        
        # Upload to MinIO
        success = uploader.upload_file(tmp_path, bucket, s3_key)
        if not success:
            raise Exception(f"Failed to upload to MinIO for {date_str} hour {hour}")
            
        print(f"Successfully processed {date_str} hour {hour} -> s3://{bucket}/{s3_key}")
        
    finally:
        # Clean up temp file
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def process_date(date_str: str, **context):
    """
    Process all 24 hours for a given date.
    
    Args:
        date_str: Date in YYYY-MM-DD format
    """
    for hour in range(24):
        download_and_upload_hour(date_str, hour)


# Create DAG
with DAG(
    'github_archive_bronze_ingestion',
    default_args=default_args,
    description='Ingest GitHub Archive data into Bronze layer',
    schedule_interval=None,  # Manually triggered for demo
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bronze', 'ingestion', 'github-archive'],
) as dag:
    
    # Get date range from environment
    start_date = os.getenv('START_DATE', '2024-01-01')
    end_date = os.getenv('END_DATE', '2024-01-03')
    
    # Create tasks for each date
    for date in generate_date_range(start_date, end_date):
        date_str = date.strftime("%Y-%m-%d")
        
        task = PythonOperator(
            task_id=f'ingest_{date_str.replace("-", "_")}',
            python_callable=process_date,
            op_kwargs={'date_str': date_str},
        )
