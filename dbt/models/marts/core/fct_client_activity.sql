{%- set force_full = (var('force_full_refresh', false) | string | lower == 'true') -%}
{{
    config(
        materialized='incremental',
        unique_key=['activity_date', 'client_id'],
        incremental_strategy='insert_overwrite' if target.type == 'bigquery' else 'delete+insert',
        partition_by=(
            {'field': 'activity_date', 'data_type': 'date', 'granularity': 'day'}
            if target.type == 'bigquery' else none
        ),
        cluster_by=['asset_class_mix'] if target.type == 'bigquery' else none,
        on_schema_change='append_new_columns',
        full_refresh=force_full,
        contract={'enforced': target.type == 'bigquery'}
    )
}}

-- Daily per-client trading activity: trade count, notional traded, platform trading revenue and client
-- money flow, enriched with acquisition channel and the regulated entity that owns the relationship.

with activity as (
    select * from {{ ref('int_client_daily_activity') }}
    {% if is_incremental() %}
        where activity_date between date '{{ var("start_date") }}' and date '{{ var("end_date") }}'
    {% endif %}
),

-- The client's dominant asset class on the day, by notional. Used as the clustering key because it
-- is the most common filter on this table and is low-cardinality.
asset_mix as (
    select
        client_id,
        activity_date,
        asset_class as asset_class_mix
    from (
        select
            client_id,
            closed_date as activity_date,
            asset_class,
            sum(notional_value) as notional,
            row_number() over (
                partition by client_id, closed_date
                order by sum(notional_value) desc, asset_class
            ) as rn
        from {{ ref('stg_trades') }}
        where closed_date is not null
        group by 1, 2, 3
    ) as ranked
    where rn = 1
),

clients as (
    select
        client_id,
        media_source,
        country,
        trading_region,
        client_category,
        platform
    from {{ ref('stg_clients') }}
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
    a.client_id,
    coalesce(c.acquisition_channel, 'Other') as acquisition_channel,
    coalesce(c.channel_type, 'Other') as channel_type,
    cl.country,
    cl.trading_region,
    cl.client_category,
    cl.platform,
    coalesce(m.asset_class_mix, 'Unknown') as asset_class_mix,
    a.trade_count,
    a.total_notional,
    a.client_pnl,
    a.spread_revenue,
    a.commission,
    a.funding_charge,
    a.trading_revenue,
    a.deposit_amount,
    a.withdrawal_amount,
    a.net_deposit
from activity as a
left join clients as cl on a.client_id = cl.client_id
left join channels as c on cl.media_source = c.media_source
left join asset_mix as m
    on a.client_id = m.client_id and a.activity_date = m.activity_date
