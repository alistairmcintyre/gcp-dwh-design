{%- set money = 'numeric' if target.type == 'bigquery' else 'decimal(18,2)' -%}
{{
    config(
        materialized='table',
        cluster_by=['trading_region'] if target.type == 'bigquery' else none,
        contract={'enforced': target.type == 'bigquery'},
        post_hook="{{ apply_row_access_policies(this) }}"
    )
}}

-- Gold client dimension: one row per registered client, carrying the KYC/PII attributes and the
-- regulatory attributes that govern what the platform may do with the relationship.
--
-- This is the governed table. Three controls apply to it, each owned by a different layer:
--   * column-level security  -- policy tags declared in `_marts__models.yml`, applied by dbt
--   * dynamic data masking   -- data policies on those tags, owned by Terraform
--   * row-level security     -- row access policies on `trading_region`, re-applied by the post-hook
--                               above because `create or replace table` drops them
-- See docs/governance.md for why the ownership is split that way.
--
-- `trading_region` is the row-access key because the divisions are separately regulated legal
-- entities. A UK desk analyst reading an EMEA client's row is not a preference violation, it is
-- a data protection one -- which is precisely why row-level security earns its place here rather
-- than splitting the table N ways.

with clients as (
    select * from {{ ref('stg_clients') }}
),

channels as (
    select
        media_source,
        acquisition_channel,
        channel_type
    from {{ ref('dim_channel_grouping') }}
),

lifetime as (
    select
        client_id,
        min(activity_date) as first_activity_date,
        max(activity_date) as last_activity_date,
        sum(trade_count) as lifetime_trade_count,
        sum(total_notional) as lifetime_notional,
        sum(trading_revenue) as lifetime_trading_revenue,
        sum(client_pnl) as lifetime_client_pnl,
        sum(net_deposit) as lifetime_net_deposit
    from {{ ref('int_client_daily_activity') }}
    group by 1
)

select
    c.client_id,
    c.first_name,
    c.last_name,
    c.date_of_birth,
    c.email,
    c.country,
    c.trading_region,
    c.client_category,
    c.account_status,
    c.account_status_reason,
    c.platform,
    c.registration_date,
    coalesce(ch.acquisition_channel, 'Other') as acquisition_channel,
    l.first_activity_date,
    l.last_activity_date,
    coalesce(l.lifetime_trade_count, 0) as lifetime_trade_count,
    cast(coalesce(l.lifetime_notional, 0) as {{ money }}) as lifetime_notional,
    cast(coalesce(l.lifetime_trading_revenue, 0) as {{ money }}) as lifetime_trading_revenue,
    cast(coalesce(l.lifetime_client_pnl, 0) as {{ money }}) as lifetime_client_pnl,
    cast(coalesce(l.lifetime_net_deposit, 0) as {{ money }}) as lifetime_net_deposit
from clients as c
left join channels as ch on c.media_source = ch.media_source
left join lifetime as l on c.client_id = l.client_id
