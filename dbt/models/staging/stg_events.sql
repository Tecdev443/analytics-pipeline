-- dbt model: staging layer for raw events
{{ config(materialized='view') }}

select
    event_id,
    user_id,
    session_id,
    event_type,
    page,
    source,
    device,
    timestamp::timestamp as event_timestamp,
    revenue,
    country,
    current_timestamp() as dbt_loaded_at
from {{ source('clickstream', 'raw_events') }}
where timestamp is not null
