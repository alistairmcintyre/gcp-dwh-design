with source as (
    select * from {{ source('raw', 'transactions') }}
)

select
    transaction_id,
    user_id,
    created_at,
    cast(created_at as date) as transaction_date,
    transaction_type,
    status as transaction_status,
    cast(amount as {{ dbt.type_numeric() }}) as amount,
    _loaded_at as loaded_at
from source
