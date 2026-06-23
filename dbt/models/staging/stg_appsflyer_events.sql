with source as (
    select * from {{ source('raw', 'appsflyer_events') }}
)

select
    event_id,
    appsflyer_id,
    user_id,
    event_name,
    event_time,
    cast(event_time as date) as event_date,
    media_source,
    campaign,
    platform,
    country,
    attributed_touch_time,
    _loaded_at as loaded_at
from source
