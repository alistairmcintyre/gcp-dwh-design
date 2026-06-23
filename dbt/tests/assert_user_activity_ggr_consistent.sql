-- GGR in the user-activity mart must equal total_stake - total_payout (allowing for rounding).
select
    activity_date,
    user_id,
    ggr,
    total_stake,
    total_payout
from {{ ref('fct_user_activity') }}
where abs(ggr - (total_stake - total_payout)) > 0.005
