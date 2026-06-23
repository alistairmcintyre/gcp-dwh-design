-- A lost bet must never return a payout. Any rows returned here are failures.
select
    bet_id,
    payout
from {{ ref('stg_bets') }}
where bet_status = 'lost'
    and payout <> 0
