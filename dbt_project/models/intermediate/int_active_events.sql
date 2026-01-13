{{
    config(
        materialized='view',
        tags=['intermediate', 'filtering']
    )
}}

/*
    Intermediate model: Filter for "active developer" signal events.
    
    This model defines what constitutes meaningful developer activity.
    
    Signal Events (included):
    - PushEvent: Code commits
    - PullRequestEvent: PR opened, merged, closed
    - IssueCommentEvent: Comments on issues/PRs
    - PullRequestReviewEvent: Code reviews
    - IssuesEvent: Issue creation
    - CreateEvent: Repository/branch creation
    
    Noise Events (excluded):
    - WatchEvent: Starring a repository (passive)
    - ForkEvent: Forking (often passive exploration)
    - MemberEvent: Team management
*/

with events as (
    select * from {{ ref('stg_github_events') }}
),

active_events as (
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
        
        -- Classify event importance
        case
            when event_type in ('PushEvent', 'PullRequestEvent', 'PullRequestReviewEvent') then 'high_signal'
            when event_type in ('IssueCommentEvent', 'IssuesEvent', 'CreateEvent') then 'medium_signal'
            else 'low_signal'
        end as signal_strength
        
    from events
    
    where 1=1
        -- Include only signal events
        and event_type in (
            'PushEvent',
            'PullRequestEvent', 
            'IssueCommentEvent',
            'PullRequestReviewEvent',
            'IssuesEvent',
            'CreateEvent'
        )
        
        -- Exclude bots (important for accurate metrics)
        and is_bot = false
        
        -- Public events only (private events don't tell us about OSS community)
        and is_public = true
)

select * from active_events
