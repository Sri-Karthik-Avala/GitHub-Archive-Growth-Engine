"""
Spark job to process Bronze layer GitHub Archive JSON data into Silver layer Parquet.

This job:
1. Reads raw JSON.gz files from MinIO Bronze bucket
2. Handles schema evolution between different years
3. Flattens nested structures and explodes arrays
4. Writes cleaned Parquet files to Silver bucket with date partitioning
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode_outer, when, coalesce, to_timestamp,
    year, month, dayofmonth, hour, lit, regexp_extract, 
    row_number, concat_ws, sha2
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    BooleanType, TimestampType, ArrayType
)
from pyspark.sql.window import Window
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session(app_name="Bronze to Silver"):
    """Create Spark session with MinIO/S3 configuration."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    return spark


def read_bronze_data(spark, bronze_path):
    """
    Read raw JSON data from Bronze layer.
    
    Args:
        spark: SparkSession
        bronze_path: S3 path to Bronze data (e.g., s3a://bronze/github_events/)
        
    Returns:
        DataFrame with raw JSON data
    """
    logger.info(f"Reading Bronze data from: {bronze_path}")
    
    # Read JSON with schema inference
    # Handle multi-line JSON objects (each line is a separate JSON object)
    df = spark.read \
        .option("multiLine", "false") \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .json(bronze_path)
    
    logger.info(f"Read {df.count()} raw records from Bronze")
    return df


def handle_schema_evolution(df):
    """
    Handle schema evolution across different years of GitHub Archive.
    
    Key changes:
    - Pre-2015: actor was a string
    - Post-2015: actor is a struct with {id, login, ...}
    - Similar changes for repo, org fields
    
    Args:
        df: Raw DataFrame
        
    Returns:
        DataFrame with standardized schema
    """
    logger.info("Handling schema evolution...")
    
    # Standardize actor fields
    df = df.withColumn(
        "actor_id",
        when(col("actor").isNotNull(), 
             coalesce(col("actor.id").cast("string"), col("actor")))
        .otherwise(None)
    ).withColumn(
        "actor_login",
        when(col("actor").isNotNull(),
             coalesce(col("actor.login"), col("actor")))
        .otherwise(None)
    )
    
    # Standardize repo fields
    df = df.withColumn(
        "repo_id",
        when(col("repo").isNotNull(),
             coalesce(col("repo.id").cast("string"), lit(None)))
        .otherwise(None)
    ).withColumn(
        "repo_name",
        when(col("repo").isNotNull(),
             coalesce(col("repo.name"), col("repository.name")))
        .otherwise(None)
    )
    
    # Standardize org fields (optional, may be null)
    df = df.withColumn(
        "org_id",
        when(col("org").isNotNull(),
             coalesce(col("org.id").cast("string"), lit(None)))
        .otherwise(None)
    ).withColumn(
        "org_login",
        when(col("org").isNotNull(),
             coalesce(col("org.login"), lit(None)))
        .otherwise(None)
    )
    
    return df


def flatten_and_clean(df):
    """
    Flatten nested structures and clean data.
    
    Args:
        df: DataFrame with standardized schema
        
    Returns:
        Cleaned DataFrame with flattened structure
    """
    logger.info("Flattening and cleaning data...")
    
    # Select and rename key fields
    df_flat = df.select(
        col("id").alias("event_id"),
        col("type").alias("event_type"),
        to_timestamp("created_at").alias("created_at"),
        col("actor_id"),
        col("actor_login"),
        col("repo_id"),
        col("repo_name"),
        col("org_id"),
        col("org_login"),
        col("public").cast("boolean").alias("is_public"),
        col("payload"),
        col("_corrupt_record")
    )
    
    # Filter out corrupt records
    df_clean = df_flat.filter(col("_corrupt_record").isNull())
    
    # Filter out null actor_id (invalid events)
    df_clean = df_clean.filter(col("actor_id").isNotNull())
    
    # Add bot detection flag
    df_clean = df_clean.withColumn(
        "is_bot",
        col("actor_login").rlike(".*\\[bot\\]$|.*-bot$|bot-.*")
    )
    
    # Add partitioning columns
    df_clean = df_clean.withColumn("event_date", col("created_at").cast("date"))
    df_clean = df_clean.withColumn("event_year", year("created_at"))
    df_clean = df_clean.withColumn("event_month", month("created_at"))
    df_clean = df_clean.withColumn("event_day", dayofmonth("created_at"))
    
    return df_clean


def deduplicate_events(df):
    """
    Deduplicate events based on event_id.
    
    Args:
        df: DataFrame to deduplicate
        
    Returns:
        Deduplicated DataFrame
    """
    logger.info("Deduplicating events...")
    
    # Create window spec to find duplicates
    window_spec = Window.partitionBy("event_id").orderBy(col("created_at").desc())
    
    # Keep only the first occurrence
    df_dedup = df.withColumn("row_num", row_number().over(window_spec)) \
                 .filter(col("row_num") == 1) \
                 .drop("row_num")
    
    return df_dedup


def write_to_silver(df, silver_path):
    """
    Write processed data to Silver layer as Parquet.
    
    Args:
        df: Processed DataFrame
        silver_path: S3 path to Silver layer (e.g., s3a://silver/github_events/)
    """
    logger.info(f"Writing to Silver layer: {silver_path}")
    
    # Drop payload column to reduce size (keep only what we need for analytics)
    # For full implementation, we'd extract specific payload fields by event type
    df_output = df.drop("payload", "_corrupt_record")
    
    # Write as Parquet with date partitioning
    # Repartition to control file count (4 files per partition for local)
    df_output.repartition(4, "event_date") \
        .write \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .parquet(silver_path)
    
    logger.info(f"Successfully wrote {df_output.count()} records to Silver layer")


def main(bronze_path, silver_path):
    """
    Main ETL pipeline from Bronze to Silver.
    
    Args:
        bronze_path: S3 path to Bronze data
        silver_path: S3 path to Silver output
    """
    spark = create_spark_session()
    
    try:
        # Read Bronze data
        df_raw = read_bronze_data(spark, bronze_path)
        
        # Handle schema evolution
        df_std = handle_schema_evolution(df_raw)
        
        # Flatten and clean
        df_clean = flatten_and_clean(df_std)
        
        # Deduplicate
        df_dedup = deduplicate_events(df_clean)
        
        # Write to Silver
        write_to_silver(df_dedup, silver_path)
        
        logger.info("Bronze to Silver processing complete!")
        
    except Exception as e:
        logger.error(f"Error in Bronze to Silver processing: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python bronze_to_silver.py <bronze_path> <silver_path>")
        print("Example: python bronze_to_silver.py s3a://bronze/github_events/ s3a://silver/github_events/")
        sys.exit(1)
    
    bronze_path = sys.argv[1]
    silver_path = sys.argv[2]
    
    main(bronze_path, silver_path)
