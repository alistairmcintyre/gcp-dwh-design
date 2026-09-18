-- One row per client money movement: deposits in and withdrawals out. Trading P&L and funding
-- charges are not transactions here -- they belong to the trade (`stg_trades`) -- so that client money
-- flow and trading revenue never get double-counted against each other.

with source as (
    select * from {{ source('raw', 'account_transactions') }}
)

select
    transaction_id,
    client_id,
    created_at,
    cast(created_at as date) as transaction_date,
    transaction_type,
    status as transaction_status,
    currency,
    cast(amount as {{ dbt.type_numeric() }}) as amount,
    _loaded_at as loaded_at
from source
