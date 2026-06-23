-- One row per user per activity date, combining betting and wallet activity.
-- Ephemeral: inlined into the downstream mart, so no physical table is created.

with bets as (
    select
        user_id,
        placed_date as activity_date,
        count(*) as bet_count,
        sum(stake) as total_stake,
        sum(payout) as total_payout,
        sum(stake) - sum(payout) as ggr
    from {{ ref('stg_bets') }}
    group by 1, 2
),

transactions as (
    select
        user_id,
        transaction_date as activity_date,
        sum(case when transaction_type = 'deposit' then amount else 0 end) as deposit_amount,
        sum(case when transaction_type = 'withdrawal' then amount else 0 end) as withdrawal_amount
    from {{ ref('stg_transactions') }}
    where transaction_status = 'completed'
    group by 1, 2
),

combined as (
    select
        coalesce(b.user_id, t.user_id) as user_id,
        coalesce(b.activity_date, t.activity_date) as activity_date,
        coalesce(b.bet_count, 0) as bet_count,
        coalesce(b.total_stake, 0) as total_stake,
        coalesce(b.total_payout, 0) as total_payout,
        coalesce(b.ggr, 0) as ggr,
        coalesce(t.deposit_amount, 0) as deposit_amount,
        coalesce(t.withdrawal_amount, 0) as withdrawal_amount
    from bets as b
    full outer join transactions as t
        on
            b.user_id = t.user_id
            and b.activity_date = t.activity_date
)

select
    user_id,
    activity_date,
    bet_count,
    total_stake,
    total_payout,
    ggr,
    deposit_amount,
    withdrawal_amount,
    deposit_amount - withdrawal_amount as net_deposit
from combined
