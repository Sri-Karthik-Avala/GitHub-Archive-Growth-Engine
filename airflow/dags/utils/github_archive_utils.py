"""
Utility functions for downloading GitHub Archive data.
"""

import os
import gzip
import json
import logging
from datetime import datetime, timedelta
from typing import Optional
import requests
import boto3
from botocore.client import Config

logger = logging.getLogger(__name__)


class GitHubArchiveDownloader:
    """Download and validate GitHub Archive data."""
    
    def __init__(self, base_url: str = "https://data.gharchive.org"):
        self.base_url = base_url
        self.session = requests.Session()
        
    def download_archive(self, date: datetime, hour: int, output_path: str) -> bool:
        """
        Download a specific hour of GitHub Archive data.
        
        Args:
            date: Date to download
            hour: Hour (0-23)
            output_path: Local path to save the file
            
        Returns:
            True if successful, False otherwise
        """
        # Format: YYYY-MM-DD-H.json.gz
        date_str = date.strftime("%Y-%m-%d")
        url = f"{self.base_url}/{date_str}-{hour}.json.gz"
        
        logger.info(f"Downloading {url}")
        
        try:
            response = self.session.get(url, stream=True, timeout=300)
            response.raise_for_status()
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Download with progress
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            logger.info(f"Downloaded {url} to {output_path}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download {url}: {e}")
            return False
    
    def validate_gzip(self, file_path: str) -> bool:
        """
        Validate that the file is a valid gzip and contains JSON.
        
        Args:
            file_path: Path to the .json.gz file
            
        Returns:
            True if valid, False otherwise
        """
        try:
            with gzip.open(file_path, 'rt') as f:
                # Read first line and try to parse as JSON
                first_line = f.readline()
                if not first_line:
                    logger.error(f"File {file_path} is empty")
                    return False
                    
                json.loads(first_line)
                logger.info(f"Validated {file_path}")
                return True
                
        except (gzip.BadGzipFile, json.JSONDecodeError) as e:
            logger.error(f"Invalid file {file_path}: {e}")
            return False


class MinIOUploader:
    """Upload files to MinIO (S3-compatible storage)."""
    
    def __init__(self, endpoint: str, access_key: str, secret_key: str):
        self.s3_client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4')
        )
        
    def create_bucket_if_not_exists(self, bucket_name: str):
        """Create a bucket if it doesn't already exist."""
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"Bucket {bucket_name} already exists")
        except:
            self.s3_client.create_bucket(Bucket=bucket_name)
            logger.info(f"Created bucket {bucket_name}")
    
    def upload_file(self, local_path: str, bucket: str, s3_key: str) -> bool:
        """
        Upload a file to MinIO/S3.
        
        Args:
            local_path: Local file path
            bucket: S3 bucket name
            s3_key: S3 object key (path in bucket)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.s3_client.upload_file(local_path, bucket, s3_key)
            logger.info(f"Uploaded {local_path} to s3://{bucket}/{s3_key}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload {local_path}: {e}")
            return False
    
    def file_exists(self, bucket: str, s3_key: str) -> bool:
        """Check if a file exists in S3."""
        try:
            self.s3_client.head_object(Bucket=bucket, Key=s3_key)
            return True
        except:
            return False


def generate_s3_path(date: datetime, hour: int, bucket: str = "bronze") -> str:
    """
    Generate partitioned S3 path for a given date and hour.
    
    Args:
        date: Date
        hour: Hour (0-23)
        bucket: Bucket name
        
    Returns:
        S3 path in format: s3://bucket/github_events/year=YYYY/month=MM/day=DD/hour=HH/data.json.gz
    """
    year = date.year
    month = f"{date.month:02d}"
    day = f"{date.day:02d}"
    hour_str = f"{hour:02d}"
    
    key = f"github_events/year={year}/month={month}/day={day}/hour={hour_str}/data.json.gz"
    return bucket, key


def generate_date_range(start_date: str, end_date: str):
    """
    Generate a list of dates between start_date and end_date.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        
    Yields:
        datetime objects
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)
