with source as (
    select * from {{ source('raw', 'bets') }}
)

select
    bet_id,
    user_id,
    placed_at,
    settled_at,
    cast(placed_at as date) as placed_date,
    sport,
    status as bet_status,
    cast(stake as {{ dbt.type_numeric() }}) as stake,
    cast(decimal_odds as {{ dbt.type_numeric() }}) as decimal_odds,
    cast(payout as {{ dbt.type_numeric() }}) as payout,
    _loaded_at as loaded_at
from source
