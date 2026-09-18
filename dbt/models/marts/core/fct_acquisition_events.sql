{%- set force_full = (var('force_full_refresh', false) | string | lower == 'true') -%}
{{
    config(
        materialized='incremental',
        unique_key=['event_date', 'acquisition_channel', 'platform'],
        incremental_strategy='insert_overwrite' if target.type == 'bigquery' else 'delete+insert',
        partition_by=(
            {'field': 'event_date', 'data_type': 'date', 'granularity': 'day'}
            if target.type == 'bigquery' else none
        ),
        on_schema_change='append_new_columns',
        full_refresh=force_full,
        contract={'enforced': target.type == 'bigquery'}
    )
}}

-- Daily acquisition funnel by channel + platform. Measures are additive counts; conversion rate
-- (registrations / installs) is intentionally left to the consumer since ratios are not additive.

with events as (
    select * from {{ ref('stg_appsflyer_events') }}
    {% if is_incremental() %}
        where event_date between date '{{ var("start_date") }}' and date '{{ var("end_date") }}'
    {% endif %}
),

channels as (
    select
        media_source,
        acquisition_channel,
        channel_type,
        is_paid
    from {{ ref('dim_channel_grouping') }}
),

joined as (
    select
        e.event_date,
        coalesce(c.acquisition_channel, 'Other') as acquisition_channel,
        coalesce(c.channel_type, 'Other') as channel_type,
        coalesce(c.is_paid, false) as is_paid,
        e.platform,
        e.event_name
    from events as e
    left join channels as c on e.media_source = c.media_source
)

select
    event_date,
    acquisition_channel,
    channel_type,
    is_paid,
    platform,
    sum(case when event_name = 'install' then 1 else 0 end) as installs,
    sum(case when event_name = 'registration' then 1 else 0 end) as registrations,
    sum(case when event_name = 'first_deposit' then 1 else 0 end) as first_deposits
from joined
group by 1, 2, 3, 4, 5
