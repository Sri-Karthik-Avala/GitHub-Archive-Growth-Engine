{{
    config(
        materialized='incremental',
        unique_key='actor_id',
        on_schema_change='append_new_columns',
        tags=['marts', 'dimension', 'incremental']
    )
}}

/*
    Dimension table: User lifecycle tracking.
    
    This incremental model maintains state for every developer ever seen,
    tracking their first and last activity dates. This is the "memory" needed
    to calculate Resurrected users.
    
    Incremental strategy: 
    - New users: INSERT
    - Existing users: UPDATE last_seen_date and event counts
*/

with new_activity as (
    select 
        actor_id,
        actor_login,
        min(created_at) as first_seen_at,
        max(created_at) as last_seen_at,
        min(event_date) as first_seen_date,
        max(event_date) as last_seen_date,
        count(*) as total_events,
        count(distinct event_date) as active_days,
        max(event_type) as last_event_type,
        max(repo_name) as last_repo
    from {{ ref('int_active_events') }}
    
    {% if is_incremental() %}
    -- Only process new data since last run
    where event_date > (select max(last_seen_date) from {{ this }})
    {% endif %}
    
    group by actor_id, actor_login
),

{% if is_incremental() %}
-- Merge with existing data
merged as (
    select
        coalesce(existing.actor_id, new_activity.actor_id) as actor_id,
        coalesce(existing.actor_login, new_activity.actor_login) as actor_login,
        coalesce(existing.first_seen_at, new_activity.first_seen_at) as first_seen_at,
        coalesce(new_activity.last_seen_at, existing.last_seen_at) as last_seen_at,
        coalesce(existing.first_seen_date, new_activity.first_seen_date) as first_seen_date,
        coalesce(new_activity.last_seen_date, existing.last_seen_date) as last_seen_date,
        coalesce(existing.total_events, 0) + coalesce(new_activity.total_events, 0) as total_events,
        coalesce(existing.active_days, 0) + coalesce(new_activity.active_days, 0) as active_days,
        coalesce(new_activity.last_event_type, existing.last_event_type) as last_event_type,
        coalesce(new_activity.last_repo, existing.last_repo) as last_repo,
        current_timestamp as updated_at
    from new_activity
    full outer join {{ this }} as existing
        on new_activity.actor_id = existing.actor_id
)

select * from merged

{% else %}
-- Initial load
select
    actor_id,
    actor_login,
    first_seen_at,
    last_seen_at,
    first_seen_date,
    last_seen_date,
    total_events,
    active_days,
    last_event_type,
    last_repo,
    current_timestamp as updated_at
from new_activity
{% endif %}
