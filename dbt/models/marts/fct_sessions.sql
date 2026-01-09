-- dbt model: fact table for user sessions
{{ config(materialized='table') }}

with sessions as (
    select
        session_id,
        user_id,
        min(event_timestamp) as session_start,
        max(event_timestamp) as session_end,
        count(*) as event_count,
        count(distinct page) as pages_visited,
        sum(revenue) as session_revenue,
        min(country) as country,
        min(source) as acquisition_source,
        min(device) as device_type
    from {{ ref('stg_events') }}
    group by 1, 2
)
select
    session_id,
    user_id,
    session_start,
    session_end,
    datediff(second, session_start, session_end) as session_duration_sec,
    event_count,
    pages_visited,
    session_revenue,
    country,
    acquisition_source,
    device_type,
    current_timestamp() as dbt_loaded_at
from sessions
