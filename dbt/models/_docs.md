{% docs bet_volume %}
**Bet volume** — the total amount staked across settled bets (`sum(stake)`), a.k.a. handle. Measures
betting activity independent of outcome.
{% enddocs %}

{% docs ggr %}
**Gross Gaming Revenue (GGR)** — `sum(stake) − sum(payout)`. What the operator keeps after paying out
winnings; the headline sportsbook revenue metric. Can be negative on a day where customers win big.
{% enddocs %}

{% docs acquisition_channel %}
**Acquisition channel** — human-friendly grouping of the AppsFlyer `media_source` that acquired a
user/device (e.g. `facebook_ads` → "Facebook Ads"), via the `dim_channel_grouping` seed.
{% enddocs %}

{% docs net_deposit %}
**Net deposit** — `deposit_amount − withdrawal_amount` over completed transactions in the period; the net
cash a cohort/user brought onto the platform.
{% enddocs %}
