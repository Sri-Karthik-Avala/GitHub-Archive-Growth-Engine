-- Create analytics database for dbt
CREATE DATABASE analytics;

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE analytics TO airflow;

-- Connect to analytics database and create schema
\c analytics

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

GRANT ALL ON SCHEMA staging TO airflow;
GRANT ALL ON SCHEMA intermediate TO airflow;
GRANT ALL ON SCHEMA marts TO airflow;
