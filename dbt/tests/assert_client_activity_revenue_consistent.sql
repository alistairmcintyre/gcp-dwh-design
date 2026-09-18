-- the platform's trading revenue is the dealing spread plus commission plus overnight funding. It is NOT the
-- client's loss -- conflating the two is the classic error in this domain, so it is asserted here.
select
    activity_date,
    client_id,
    trading_revenue,
    spread_revenue,
    commission,
    funding_charge
from {{ ref('fct_client_activity') }}
where abs(trading_revenue - (spread_revenue + commission + funding_charge)) > 0.005
