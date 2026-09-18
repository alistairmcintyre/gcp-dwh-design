{{ config(group='core') }}

-- Belongs to the `core` group: it asserts a Core Lake invariant across two staging models and may
-- therefore reference `access: private` models. A domain's own tests cannot.
--
-- A FILLED or PART_FILLED order must have produced at least one execution. An order that reports
-- filled with no trade behind it is a reconciliation break between the order book and the execution
-- feed -- the kind of gap that shows up as a client complaint rather than a failed test.
select
    o.order_id,
    o.order_status
from {{ ref('stg_orders') }} as o
left join {{ ref('stg_trades') }} as t on o.order_id = t.order_id
where
    o.order_status in ('FILLED', 'PART_FILLED')
    and t.trade_id is null
