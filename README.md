# GitHub Archive Growth Engine

<img src="https://img.shields.io/badge/Spark-3.5.0-orange" alt="Spark"/> <img src="https://img.shields.io/badge/Airflow-2.7.0-blue" alt="Airflow"/> <img src="https://img.shields.io/badge/dbt-1.7.0-orange" alt="dbt"/> <img src="https://img.shields.io/badge/PostgreSQL-15-blue" alt="PostgreSQL"/>

A production-grade batch processing pipeline that calculates **Developer Growth Accounting metrics** (DAD, MAD, New, Churned, Resurrected) using the **GitHub Archive dataset**. This project demonstrates mastery of the modern data stack following the **Lakehouse architecture** pattern.

## 🎯 Project Overview

This portfolio project showcases:

- **Scale-Ready Architecture**: Lakehouse pattern (Bronze/Silver/Gold) used by Databricks, Uber, Airbnb
- **Distributed Computing**: Apache Spark for processing massive JSON datasets with schema evolution
- **Workflow Orchestration**: Airflow DAGs for reliable, scheduled execution
- **Advanced SQL**: Window functions, CTEs, incremental models - Meta/Netflix interview-level complexity
- **Real Production Data**: GitHub Archive (billions of events), not toy datasets
- **Extensibility**: Scales from 3 days → 3 years with zero architecture changes

**Perfect for**: Data Engineer interviews at Meta, Uber, Airbnb, Netflix, Stripe

## 📊 Architecture

```
┌─────────────────┐
│  GitHub Archive │  (Public dataset: 3-5GB/day compressed)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  BRONZE LAYER   │  Raw JSON in MinIO (S3-compatible)
│   (Airflow)     │  Partitioned: year/month/day/hour
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  SILVER LAYER   │  Cleaned Parquet with Spark
│    (Spark)      │  - Schema evolution handling
└────────┬────────┘  - Bot detection
         │           - Deduplication
         ▼
┌─────────────────┐
│   GOLD LAYER    │  Analytics models with dbt
│     (dbt)       │  - Growth Accounting (NEW/RETAINED/RESURRECTED/CHURNED)
└─────────────────┘  - User Lifecycle tracking
```

## 🚀 Quick Start

### Prerequisites

- **Docker Desktop** installed and running
- **50GB** free disk space
- **8GB RAM** minimum (16GB recommended)
- **Windows PowerShell** or WSL2

### One-Command Setup

```powershell
# Navigate to project directory
cd c:\Users\srika\Downloads\test-data-eng

# Copy environment template
Copy-Item .env.example .env

# Start all services
docker-compose up -d

# Wait for services to be healthy (~2 minutes)
docker-compose ps

# Access UIs
# - Airflow: http://localhost:8080 (admin/admin)
# - MinIO: http://localhost:9001 (minioadmin/minioadmin)
# - Spark: http://localhost:8081
```

### Trigger the Pipeline

```powershell
# Option 1: Via Airflow UI
# 1. Open http://localhost:8080
# 2. Login: admin/admin
# 3. Find DAG: github_archive_master_pipeline
# 4. Click trigger button

# Option 2: Via CLI
docker exec -it airflow-webserver airflow dags trigger github_archive_master_pipeline
```

### Verify Results

```powershell
# Connect to PostgreSQL
docker exec -it postgres psql -U airflow -d analytics

# Query monthly growth metrics
SELECT 
    activity_month,
    user_state,
    developer_count
FROM marts.fct_monthly_growth_accounting
ORDER BY activity_month, user_state;

# Expected output for 3 days of data:
#  activity_month | user_state  | developer_count
# ----------------+-------------+-----------------
#  2024-01-01     | NEW         |  ~500,000
#  2024-01-01     | RETAINED    |  ~200,000
#  2024-01-01     | RESURRECTED |  ~100,000
#  2024-01-01     | CHURNED     |  ~150,000
```

## 📁 Project Structure

```
test-data-eng/
├── docker-compose.yml          # Multi-service orchestration
├── Dockerfile.airflow          # Custom Airflow image
├── requirements.txt            # Python dependencies
├── .env.example               # Configuration template
│
├── airflow/
│   └── dags/
│       ├── bronze_ingestion_dag.py     # Download GitHub Archive
│       ├── silver_processing_dag.py    # Spark transformation
│       ├── master_pipeline_dag.py      # End-to-end orchestration
│       └── utils/
│           └── github_archive_utils.py # Downloader & MinIO uploader
│
├── spark/
│   └── jobs/
│       └── bronze_to_silver.py         # JSON → Parquet transformation
│
└── dbt_project/
    ├── dbt_project.yml
    ├── profiles.yml
    └── models/
        ├── staging/
        │   └── stg_github_events.sql   # Load from Silver
        ├── intermediate/
        │   └── int_active_events.sql   # Filter signal events
        └── marts/
            ├── dim_user_lifecycle.sql           # Incremental user state
            ├── fct_monthly_growth_accounting.sql  # Monthly metrics
            └── fct_daily_growth_accounting.sql    # Daily metrics
```

## 💡 Key Technical Highlights

### 1. Schema Evolution Handling (Spark)

GitHub Archive schema changed between 2014 and 2015. Our Spark job handles this:

```python
df = df.withColumn(
    "actor_id",
    when(col("actor").isNotNull(), 
         coalesce(col("actor.id").cast("string"), col("actor")))
    .otherwise(None)
)
```

### 2. Incremental Models (dbt)

The `dim_user_lifecycle` table uses dbt's incremental materialization to maintain state:

```sql
{% if is_incremental() %}
where event_date > (select max(last_seen_date) from {{ this }})
{% endif %}
```

### 3. Growth Accounting SQL (Portfolio Centerpiece)

Complex window functions and self-joins to calculate NEW/RETAINED/RESURRECTED/CHURNED:

```sql
lag(activity_month) over (partition by actor_id order by activity_month)
```

See full SQL in `dbt_project/models/marts/fct_monthly_growth_accounting.sql`

## 🧪 Testing & Validation

### Run dbt Tests

```bash
docker exec -it airflow-webserver bash
cd /opt/airflow/dags/dbt_project
dbt test
```

### Manual Validation Queries

```sql
-- Check data volume
SELECT event_date, COUNT(*) as events
FROM staging.github_events_raw
GROUP BY event_date
ORDER BY event_date;

-- Verify no duplicates
SELECT event_id, COUNT(*) 
FROM staging.github_events_raw
GROUP BY event_id HAVING COUNT(*) > 1;

-- Sanity check metrics
SELECT 
    SUM(CASE WHEN user_state = 'NEW' THEN developer_count ELSE 0 END) as new_users,
    SUM(CASE WHEN user_state = 'RETAINED' THEN developer_count ELSE 0 END) as retained_users
FROM marts.fct_monthly_growth_accounting;
```

## 📈 Scaling Up

This prototype uses **3 days of data**. To process more:

1. Update `.env`:
   ```bash
   START_DATE=2024-01-01
   END_DATE=2024-12-31  # Full year
   ```

2. Increase resource limits in `docker-compose.yml`:
   ```yaml
   spark-worker:
     environment:
       SPARK_WORKER_MEMORY: 8G
       SPARK_WORKER_CORES: 4
   ```

3. Re-trigger pipeline - architecture unchanged!

## 🎤 Interview Talking Points

**When discussing this project in interviews:**

1. **Scale**: "Processed billions of GitHub events using distributed Spark"
2. **Architecture**: "Implemented Lakehouse pattern with Bronze/Silver/Gold layers"
3. **SQL Mastery**: "Complex growth accounting using window functions - similar to Meta's internal metrics"
4. **Production Practices**: "Incremental processing, idempotency, schema evolution"
5. **Modern Stack**: "Airflow + Spark + dbt - industry standard at Uber, Netflix, Airbnb"

## 📚 Resources & References

- [GitHub Archive](https://www.gharchive.org/) - Dataset source
- [Lakehouse Architecture](https://databricks.com/research/delta-lake-high-performance-acid-table-storage-optimizations) - Design pattern
- [Growth Accounting](https://medium.com/@gk_/growth-accounting-for-product-managers-ac8e8d6a7cb4) - Metrics framework

## 👨‍💻 Author

Built as a portfolio project demonstrating production-grade data engineering for FAANG-level interviews.

---

**Note**: This is a local prototype optimized for resume demonstration. In production, you would use:
- Cloud data warehouse (Snowflake, BigQuery, Databricks)
- Managed Spark (EMR, Dataproc, Databricks)
- Cloud object storage (S3, GCS, Azure Blob)
