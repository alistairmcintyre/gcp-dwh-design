{{ config(group='core') }}

-- A CANCELLED or REJECTED order never reached the market, so it cannot have executions against it.
-- Any rows returned here are failures.
select
    o.order_id,
    o.order_status,
    t.trade_id
from {{ ref('stg_orders') }} as o
inner join {{ ref('stg_trades') }} as t on o.order_id = t.order_id
where o.order_status in ('CANCELLED', 'REJECTED')
