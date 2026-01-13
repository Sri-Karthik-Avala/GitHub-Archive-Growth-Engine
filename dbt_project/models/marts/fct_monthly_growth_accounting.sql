{{
    config(
        materialized='table',
        tags=['marts', 'fact', 'growth']
    )
}}

/*
    Fact table: Monthly Growth Accounting
    
    This is the "crown jewel" SQL that demonstrates mastery of:
    - Window functions (LAG, MIN OVER)
    - Temporal logic and date arithmetic
    - Growth accounting frameworks (Meta/Facebook style)
    - Complex CTEs and self-joins
    
    Metrics calculated:
    - NEW: Users active this month for the first time ever
    - RETAINED: Users active this month AND active last month
    - RESURRECTED: Users active this month but NOT active last month (churned back in)
    - CHURNED: Users active last month but NOT active this month
    - MAD (Monthly Active Developers): NEW + RETAINED + RESURRECTED
*/

with monthly_activity as (
    -- Get unique users active each month
    select distinct
        actor_id,
        date_trunc('month', event_date)::date as activity_month
    from {{ ref('int_active_events') }}
),

user_timeline as (
    -- Build temporal timeline for each user
    select
        actor_id,
        activity_month,
        -- Look back at previous month's activity
        lag(activity_month) over (
            partition by actor_id 
            order by activity_month
        ) as previous_activity_month,
        -- Track when user was first ever seen
        min(activity_month) over (
            partition by actor_id
        ) as first_activity_month
    from monthly_activity
),

classified_users as (
    -- Classify each user's state for each month they're active
    select
        activity_month,
        actor_id,
        case
            -- NEW: Active this month and it's their first month ever
            when activity_month = first_activity_month then 'NEW'
            
            -- RETAINED: Active this month AND active last month (continuous)
            when previous_activity_month = activity_month - interval '1 month' then 'RETAINED'
            
            -- RESURRECTED: Active this month but previous activity was >1 month ago
            when previous_activity_month < activity_month - interval '1 month' 
                or previous_activity_month is null  -- edge case for first month
                then case
                    when activity_month = first_activity_month then 'NEW'  -- Can't be resurrected if it's first month
                    else 'RESURRECTED'
                end
            
            else 'UNKNOWN'  -- Should not happen
        end as user_state
    from user_timeline
),

churned_users as (
    /*
        CHURNED calculation: Users active in month M who are NOT active in month M+1.
        
        This requires looking forward, not backward. We do a self-join to find
        users who were active in month M but not in month M+1.
    */
    select
        (t1.activity_month + interval '1 month')::date as activity_month,
        t1.actor_id,
        'CHURNED' as user_state
    from user_timeline t1
    left join user_timeline t2 
        on t1.actor_id = t2.actor_id 
        and t2.activity_month = t1.activity_month + interval '1 month'
    where t2.actor_id is null  -- No activity in next month = churned
),

combined as (
    -- Combine active users and churned users
    select * from classified_users
    union all
    select * from churned_users
),

final as (
    select 
        activity_month,
        user_state,
        count(distinct actor_id) as developer_count
    from combined
    group by activity_month, user_state
)

select 
    activity_month,
    user_state,
    developer_count,
    
    -- Add cumulative metrics for context
    sum(developer_count) over (
        partition by activity_month
    ) as total_events_this_month,
    
    -- Calculate derived metrics
    case 
        when user_state in ('NEW', 'RETAINED', 'RESURRECTED') 
        then sum(developer_count) over (
            partition by activity_month
            order by user_state
            rows between unbounded preceding and unbounded following
        )
    end as mad  -- Monthly Active Developers
    
from final
order by activity_month, user_state
