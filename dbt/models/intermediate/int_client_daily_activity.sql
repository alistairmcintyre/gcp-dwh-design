-- One row per client per activity date, combining trading and client-money activity.
-- Ephemeral: inlined into the downstream mart, so no physical table is created.
--
-- Trades are attributed to their CLOSE date, not their open date: revenue is realised on close, and a
-- position held across several days must not inflate activity on each of them. Positions still open have
-- no `closed_date` and are therefore absent -- unrealised P&L is a risk measure, not a revenue one.

with trades as (
    select
        client_id,
        closed_date as activity_date,
        count(*) as trade_count,
        sum(notional_value) as total_notional,
        sum(client_pnl) as client_pnl,
        sum(spread_revenue) as spread_revenue,
        sum(commission) as commission,
        sum(funding_charge) as funding_charge,
        sum(trading_revenue) as trading_revenue
    from {{ ref('stg_trades') }}
    where closed_date is not null
    group by 1, 2
),

transactions as (
    select
        client_id,
        transaction_date as activity_date,
        sum(case when transaction_type = 'deposit' then amount else 0 end) as deposit_amount,
        sum(case when transaction_type = 'withdrawal' then amount else 0 end) as withdrawal_amount
    from {{ ref('stg_account_transactions') }}
    where transaction_status = 'completed'
    group by 1, 2
),

combined as (
    select
        coalesce(d.client_id, t.client_id) as client_id,
        coalesce(d.activity_date, t.activity_date) as activity_date,
        coalesce(d.trade_count, 0) as trade_count,
        coalesce(d.total_notional, 0) as total_notional,
        coalesce(d.client_pnl, 0) as client_pnl,
        coalesce(d.spread_revenue, 0) as spread_revenue,
        coalesce(d.commission, 0) as commission,
        coalesce(d.funding_charge, 0) as funding_charge,
        coalesce(d.trading_revenue, 0) as trading_revenue,
        coalesce(t.deposit_amount, 0) as deposit_amount,
        coalesce(t.withdrawal_amount, 0) as withdrawal_amount
    from trades as d
    full outer join transactions as t
        on
            d.client_id = t.client_id
            and d.activity_date = t.activity_date
)

select
    client_id,
    activity_date,
    trade_count,
    total_notional,
    client_pnl,
    spread_revenue,
    commission,
    funding_charge,
    trading_revenue,
    deposit_amount,
    withdrawal_amount,
    deposit_amount - withdrawal_amount as net_deposit
from combined
