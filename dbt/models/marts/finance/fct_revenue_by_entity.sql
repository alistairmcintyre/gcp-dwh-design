{{
    config(
        materialized='table',
        group='finance',
        access='private',
        schema='marts_finance'
    )
}}

-- Finance domain mart: trading revenue by regulated entity and asset class.
--
-- Grain: activity_date x trading_region x asset_class_mix.
--
-- Revenue is split into its three components rather than reported as a single figure, because
-- Finance recognises them differently: spread and commission are transaction revenue, overnight
-- funding is financing income. A single `trading_revenue` column would force that split to be
-- re-derived in a spreadsheet, which is where revenue definitions go to diverge.
--
-- Like the compliance mart, this refs only `access: public` core models.

with activity as (
    select * from {{ ref('fct_client_activity') }}
)

select
    activity_date,
    trading_region,
    asset_class_mix as asset_class,
    count(distinct client_id) as active_clients,
    sum(trade_count) as trade_count,
    sum(total_notional) as total_notional,
    sum(spread_revenue) as spread_revenue,
    sum(commission) as commission,
    sum(funding_charge) as funding_charge,
    sum(trading_revenue) as trading_revenue,
    sum(client_pnl) as client_pnl
from activity
group by 1, 2, 3
