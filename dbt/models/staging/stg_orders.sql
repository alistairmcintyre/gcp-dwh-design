-- One row per client instruction.
--
-- ORDERS AND TRADES ARE DIFFERENT THINGS. An order is what the client asked for; a trade is what the
-- market gave them. The relationship is 1:0..n -- a working order may never fill, and a large one
-- commonly fills in several pieces at different prices. Collapsing the two into a single entity is
-- the classic modelling error in this domain: it makes partial fills invisible and average execution
-- price impossible to compute honestly.

with source as (
    select * from {{ source('raw', 'orders') }}
)

select
    order_id,
    client_id,
    instrument_id,
    side,
    order_type,
    status as order_status,
    placed_at,
    cast(placed_at as date) as placed_date,
    currency,
    cast(quantity as {{ dbt.type_numeric() }}) as quantity,
    cast(limit_price as {{ dbt.type_numeric() }}) as limit_price,
    cast(stop_price as {{ dbt.type_numeric() }}) as stop_price,
    _loaded_at as loaded_at
from source
