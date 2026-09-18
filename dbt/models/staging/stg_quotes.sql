-- Sampled bid/ask ticks per instrument.
--
-- Kept because execution quality is a regulatory question, not an analytics one: evidencing best
-- execution means comparing the fill price against the quote prevailing at the moment of execution.
-- Sampled rather than tick-for-tick -- the full feed belongs on the trading platform.

with source as (
    select * from {{ source('raw', 'quotes') }}
)

select
    instrument_id,
    quote_time,
    cast(quote_time as date) as quote_date,
    cast(bid as {{ dbt.type_numeric() }}) as bid,
    cast(ask as {{ dbt.type_numeric() }}) as ask,
    cast(mid as {{ dbt.type_numeric() }}) as mid,
    cast(ask - bid as {{ dbt.type_numeric() }}) as spread,
    _loaded_at as loaded_at
from source
