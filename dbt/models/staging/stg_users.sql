with source as (
    select * from {{ source('raw', 'users') }}
)

select
    user_id,
    appsflyer_id,
    registration_time,
    cast(registration_time as date) as registration_date,
    country,
    acquisition_media_source as media_source,
    platform,
    _loaded_at as loaded_at
from source
