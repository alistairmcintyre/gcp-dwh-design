-- A void bet refunds exactly the stake (payout = stake). Any rows returned here are failures.
select
    bet_id,
    stake,
    payout
from {{ ref('stg_bets') }}
where bet_status = 'void'
    and payout <> stake
