{{
    config(
        materialized='view',
        tags=['staging', 'github']
    )
}}

/*
    Staging model: Load Silver Parquet data into PostgreSQL.
    
    This model reads from an external table that points to the Silver Parquet files.
    In a real implementation, you would either:
    1. Use a foreign data wrapper (e.g., parquet_fdw for PostgreSQL)
    2. Load Parquet data into PostgreSQL via batch import (Spark -> JDBC)
    3. Use a data warehouse with native Parquet support (Snowflake, BigQuery, Databricks)
    
    For this prototype, we assume data has been loaded into a staging.github_events_raw table.
*/

with source as (
    select * from {{ source('staging', 'github_events_raw') }}
),

renamed as (
    select
        event_id,
        event_type,
        created_at,
        actor_id,
        actor_login,
        repo_id,
        repo_name,
        org_id,
        org_login,
        is_public,
        is_bot,
        event_date,
        event_year,
        event_month,
        event_day
    from source
)

select * from renamed
