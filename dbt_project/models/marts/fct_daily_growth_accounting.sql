{{
    config(
        materialized='table',
        tags=['marts', 'fact', 'growth', 'daily']
    )
}}

/*
    Fact table: Daily Active Developers (DAD)
    
    Similar to monthly growth accounting, but at daily granularity.
    Useful for tracking shorter-term trends and engagement patterns.
*/

with daily_activity as (
    select distinct
        actor_id,
        event_date as activity_date
    from {{ ref('int_active_events') }}
),

user_timeline as (
    select
        actor_id,
        activity_date,
        lag(activity_date) over (
            partition by actor_id 
            order by activity_date
        ) as previous_activity_date,
        min(activity_date) over (
            partition by actor_id
        ) as first_activity_date
    from daily_activity
),

classified_users as (
    select
        activity_date,
        actor_id,
        case
            when activity_date = first_activity_date then 'NEW'
            when previous_activity_date = activity_date - interval '1 day' then 'RETAINED'
            when previous_activity_date < activity_date - interval '1 day' 
                or previous_activity_date is null  
                then case
                    when activity_date = first_activity_date then 'NEW'
                    else 'RESURRECTED'
                end
            else 'UNKNOWN'
        end as user_state
    from user_timeline
)

select 
    activity_date,
    user_state,
    count(distinct actor_id) as developer_count
from classified_users
group by activity_date, user_state
order by activity_date, user_state
