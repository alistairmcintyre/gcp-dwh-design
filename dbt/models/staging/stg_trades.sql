-- One row per trade (a closed position). A trade is the unit of trading activity: a client opens a
-- position on an instrument, and it closes by client instruction, a stop/limit, or a margin close-out.
--
-- NOTE ON REVENUE. `client_pnl` is the *client's* realised profit or loss. It is not the platform's revenue.
-- the platform's revenue on a trade is `spread_revenue + commission + funding_charge` -- the dealing spread,
-- any explicit commission (share CFDs), and overnight funding on positions held past cut-off.
-- Conflating the two is the classic modelling error in this domain, so they are kept separate here
-- and only combined in `trading_revenue`.

with source as (
    select * from {{ source('raw', 'trades') }}
)

select
    trade_id,
    order_id,
    client_id,
    instrument_id,
    market_name,
    asset_class,
    product_type,
    direction,
    opened_at,
    closed_at,
    cast(opened_at as date) as opened_date,
    cast(closed_at as date) as closed_date,
    status as trade_status,
    currency,
    cast(quantity as {{ dbt.type_numeric() }}) as quantity,
    cast(opening_price as {{ dbt.type_numeric() }}) as opening_price,
    cast(closing_price as {{ dbt.type_numeric() }}) as closing_price,
    cast(notional_value as {{ dbt.type_numeric() }}) as notional_value,
    cast(client_pnl as {{ dbt.type_numeric() }}) as client_pnl,
    cast(spread_revenue as {{ dbt.type_numeric() }}) as spread_revenue,
    cast(commission as {{ dbt.type_numeric() }}) as commission,
    cast(funding_charge as {{ dbt.type_numeric() }}) as funding_charge,
    cast(spread_revenue + commission + funding_charge as {{ dbt.type_numeric() }}) as trading_revenue,
    _loaded_at as loaded_at
from source
