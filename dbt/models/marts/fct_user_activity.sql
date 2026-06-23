{%- set force_full = (var('force_full_refresh', false) | string | lower == 'true') -%}
{{
    config(
        materialized='incremental',
        unique_key=['activity_date', 'user_id'],
        incremental_strategy='insert_overwrite' if target.type == 'bigquery' else 'delete+insert',
        partition_by=(
            {'field': 'activity_date', 'data_type': 'date', 'granularity': 'day'}
            if target.type == 'bigquery' else none
        ),
        cluster_by=['acquisition_channel'] if target.type == 'bigquery' else none,
        on_schema_change='append_new_columns',
        full_refresh=force_full,
        contract={'enforced': target.type == 'bigquery'}
    )
}}

-- Daily per-user activity: bet volume, GGR, deposits and withdrawals, enriched with acquisition channel.

with activity as (
    select * from {{ ref('int_user_daily_activity') }}
    {% if is_incremental() %}
        where activity_date between date '{{ var("start_date") }}' and date '{{ var("end_date") }}'
    {% endif %}
),

users as (
    select
        user_id,
        media_source,
        country,
        platform
    from {{ ref('stg_users') }}
),

channels as (
    select
        media_source,
        acquisition_channel,
        channel_type
    from {{ ref('dim_channel_grouping') }}
)

select
    a.activity_date,
    a.user_id,
    coalesce(c.acquisition_channel, 'Other') as acquisition_channel,
    coalesce(c.channel_type, 'Other') as channel_type,
    u.country,
    u.platform,
    a.bet_count,
    a.total_stake,
    a.total_payout,
    a.ggr,
    a.deposit_amount,
    a.withdrawal_amount,
    a.net_deposit
from activity as a
left join users as u on a.user_id = u.user_id
left join channels as c on u.media_source = c.media_source
