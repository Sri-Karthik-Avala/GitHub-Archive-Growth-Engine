# Sample SQL Queries for Portfolio Demonstration

## Growth Metrics Analysis

### Monthly Active Developers (MAD) Trend
```sql
-- Show MAD growth over time
SELECT 
    activity_month,
    SUM(CASE WHEN user_state IN ('NEW', 'RETAINED', 'RESURRECTED') 
        THEN developer_count ELSE 0 END) as mad,
    SUM(CASE WHEN user_state = 'NEW' THEN developer_count ELSE 0 END) as new_developers,
    SUM(CASE WHEN user_state = 'RETAINED' THEN developer_count ELSE 0 END) as retained_developers,
    SUM(CASE WHEN user_state = 'RESURRECTED' THEN developer_count ELSE 0 END) as resurrected_developers,
    SUM(CASE WHEN user_state = 'CHURNED' THEN developer_count ELSE 0 END) as churned_developers
FROM marts.fct_monthly_growth_accounting
GROUP BY activity_month
ORDER BY activity_month;
```

### Retention Rate Calculation
```sql
-- Calculate monthly retention rate
WITH monthly_totals AS (
    SELECT 
        activity_month,
        SUM(CASE WHEN user_state = 'RETAINED' THEN developer_count ELSE 0 END) as retained,
        SUM(CASE WHEN user_state IN ('NEW', 'RETAINED', 'RESURRECTED') 
            THEN developer_count ELSE 0 END) as total_active
    FROM marts.fct_monthly_growth_accounting
    GROUP BY activity_month
)
SELECT 
    activity_month,
    retained,
    total_active,
    ROUND(100.0 * retained / NULLIF(total_active, 0), 2) as retention_rate_pct
FROM monthly_totals
ORDER BY activity_month;
```

### Cohort Analysis - First Month Retention
```sql
-- Track what % of new developers in Month 1 are still active in Month 2, 3, etc.
WITH first_month_cohort AS (
    SELECT DISTINCT actor_id
    FROM marts.dim_user_lifecycle
    WHERE first_seen_date BETWEEN '2024-01-01' AND '2024-01-31'
),
monthly_activity_flags AS (
    SELECT 
        fmc.actor_id,
        DATE_TRUNC('month', ae.event_date)::date as activity_month
    FROM first_month_cohort fmc
    LEFT JOIN intermediate.int_active_events ae 
        ON fmc.actor_id = ae.actor_id
    GROUP BY fmc.actor_id, DATE_TRUNC('month', ae.event_date)
)
SELECT 
    activity_month,
    COUNT(DISTINCT actor_id) as active_from_cohort,
    (SELECT COUNT(*) FROM first_month_cohort) as cohort_size,
    ROUND(100.0 * COUNT(DISTINCT actor_id) / (SELECT COUNT(*) FROM first_month_cohort), 2) as retention_pct
FROM monthly_activity_flags
GROUP BY activity_month
ORDER BY activity_month;
```

## Event Analysis

### Most Active Event Types
```sql
SELECT 
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT actor_id) as unique_developers,
    ROUND(AVG(CASE WHEN signal_strength = 'high_signal' THEN 1.0 ELSE 0.0 END), 3) as high_signal_ratio
FROM intermediate.int_active_events
GROUP BY event_type
ORDER BY event_count DESC
LIMIT 10;
```

### Daily Activity Pattern
```sql
-- Show day-of-week pattern
SELECT 
    EXTRACT(DOW FROM event_date) as day_of_week,
    CASE EXTRACT(DOW FROM event_date)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END as day_name,
    COUNT(*) as total_events,
    COUNT(DISTINCT actor_id) as unique_developers
FROM intermediate.int_active_events
GROUP BY EXTRACT(DOW FROM event_date)
ORDER BY day_of_week;
```

## User Lifecycle Analysis

### Power Users - Most Active Developers
```sql
SELECT 
    actor_id,
    actor_login,
    total_events,
    active_days,
    ROUND(total_events::numeric / NULLIF(active_days, 0), 2) as avg_events_per_active_day,
    first_seen_date,
    last_seen_date,
    last_repo
FROM marts.dim_user_lifecycle
ORDER BY total_events DESC
LIMIT 50;
```

### Resurrection Rate by Dormancy Period
```sql
-- How likely are users to return based on how long they've been away?
WITH user_gaps AS (
    SELECT 
        actor_id,
        event_date as return_date,
        LAG(event_date) OVER (PARTITION BY actor_id ORDER BY event_date) as previous_date,
        event_date - LAG(event_date) OVER (PARTITION BY actor_id ORDER BY event_date) as days_dormant
    FROM (
        SELECT DISTINCT actor_id, event_date
        FROM intermediate.int_active_events
    ) t
)
SELECT 
    CASE 
        WHEN days_dormant <= 7 THEN '1-7 days'
        WHEN days_dormant <= 30 THEN '8-30 days'
        WHEN days_dormant <= 90 THEN '31-90 days'
        WHEN days_dormant <= 180 THEN '91-180 days'
        ELSE '180+ days'
    END as dormancy_bucket,
    COUNT(*) as resurrection_count,
    COUNT(DISTINCT actor_id) as unique_resurrected_users
FROM user_gaps
WHERE days_dormant > 1  -- Exclude consecutive day activity
GROUP BY dormancy_bucket
ORDER BY MIN(days_dormant);
```

## Data Quality Checks

### Event Volume by Date
```sql
SELECT 
    event_date,
    COUNT(*) as total_events,
    COUNT(DISTINCT actor_id) as unique_actors,
    COUNT(DISTINCT repo_name) as unique_repos,
    COUNT(DISTINCT event_type) as event_type_variety
FROM staging.stg_github_events
GROUP BY event_date
ORDER BY event_date;
```

### Bot Detection Stats
```sql
SELECT 
    is_bot,
    COUNT(*) as event_count,
    COUNT(DISTINCT actor_id) as unique_actors,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct_of_total
FROM staging.stg_github_events
GROUP BY is_bot;
```

### Duplicate Check
```sql
-- Verify no duplicates after deduplication
SELECT 
    event_id,
    COUNT(*) as occurrence_count
FROM staging.stg_github_events
GROUP BY event_id
HAVING COUNT(*) > 1;
-- Should return 0 rows
```

## Interview Demo Query

### "The Money Query" - Full Growth Accounting with Derived Metrics
```sql
-- This single query shows everything: NEW, RETAINED, RESURRECTED, CHURNED, MAD, Retention%
WITH monthly_metrics AS (
    SELECT 
        activity_month,
        user_state,
        developer_count
    FROM marts.fct_monthly_growth_accounting
),
pivoted AS (
    SELECT 
        activity_month,
        SUM(CASE WHEN user_state = 'NEW' THEN developer_count ELSE 0 END) as new_developers,
        SUM(CASE WHEN user_state = 'RETAINED' THEN developer_count ELSE 0 END) as retained_developers,
        SUM(CASE WHEN user_state = 'RESURRECTED' THEN developer_count ELSE 0 END) as resurrected_developers,
        SUM(CASE WHEN user_state = 'CHURNED' THEN developer_count ELSE 0 END) as churned_developers
    FROM monthly_metrics
    GROUP BY activity_month
)
SELECT 
    activity_month,
    new_developers,
    retained_developers,
    resurrected_developers,
    churned_developers,
    (new_developers + retained_developers + resurrected_developers) as mad,
    -- Retention rate
    ROUND(100.0 * retained_developers / NULLIF(
        LAG(new_developers + retained_developers + resurrected_developers) 
        OVER (ORDER BY activity_month), 0
    ), 2) as retention_rate_pct,
    -- Quick ratio (growth efficiency)
    ROUND((new_developers + resurrected_developers)::numeric / 
          NULLIF(churned_developers, 0), 2) as quick_ratio,
    -- Month-over-month growth
    ROUND(100.0 * (
        (new_developers + retained_developers + resurrected_developers) - 
        LAG(new_developers + retained_developers + resurrected_developers) 
        OVER (ORDER BY activity_month)
    ) / NULLIF(
        LAG(new_developers + retained_developers + resurrected_developers) 
        OVER (ORDER BY activity_month), 0
    ), 2) as mom_growth_pct
FROM pivoted
ORDER BY activity_month;
```

**Use in Interview**:
> "This query demonstrates everything we've built: growth accounting, retention calculation, and derived metrics like Quick Ratio (borrowed from SaaS) to measure growth efficiency. The window functions allow us to calculate month-over-month changes and retention rates in a single pass."
