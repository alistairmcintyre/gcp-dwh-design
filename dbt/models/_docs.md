{% docs notional_volume %}
**Notional volume.** The total notional value of trades closed in the period (`sum(notional_value)`),
where notional is trade size multiplied by the price of the underlying. Measures trading activity
independent of outcome, and is the denominator for most revenue-capture ratios.
{% enddocs %}

{% docs trading_revenue %}
**Trading revenue.** `spread_revenue + commission + funding_charge`, the platform's revenue on a trade: the
dealing spread, any explicit commission (share CFDs), and overnight funding on positions held past
cut-off.

**This is not the client's loss.** Client P&L (`client_pnl`) is a separate measure and moves
independently, so a client can close in profit on a trade that still earned the platform the full spread.
Conflating the two is the classic modelling error in this domain, which is why they are carried as
distinct columns and reconciled by a singular test.
{% enddocs %}

{% docs client_pnl %}
**Client P&L.** The client's realised profit or loss on closed trades, in account currency. Negative
values mean the client lost. Reported for client-outcome and suitability analysis, never as a revenue
measure; see `trading_revenue`.
{% enddocs %}

{% docs acquisition_channel %}
**Acquisition channel.** Human-friendly grouping of the AppsFlyer `media_source` that acquired a
client/device (e.g. `facebook_ads` → "Facebook Ads"), via the `dim_channel_grouping` seed.
{% enddocs %}

{% docs net_deposit %}
**Net deposit.** `deposit_amount − withdrawal_amount` over completed client-money transactions in the
period; the net cash a cohort/client brought onto the platform. Trading P&L and funding charges are
deliberately excluded, since they belong to the trade rather than to client money flow.
{% enddocs %}

{% docs client_category %}
**Client categorisation.** MiFID II classification: `RETAIL`, `PROFESSIONAL`, or
`ELECTIVE_PROFESSIONAL`. Determines leverage caps, negative balance protection and the disclosures a
client is entitled to, so it is a regulatory attribute rather than a marketing segment, and a change
to it has downstream compliance consequences.
{% enddocs %}

{% docs account_status %}
**Account status.** Lifecycle state of the trading account: `ACTIVE`, `RESTRICTED`, `SUSPENDED`,
`DORMANT` or `CLOSED`. Paired with `account_status_reason`, which carries the cause, an
appropriateness failure, a vulnerability flag, expired KYC, or client request.

This attribute is the reason the client-status topic has a sub-minute delivery SLA rather than an
hourly one: a restricted or vulnerable client still present in a marketing audience is a regulatory
breach, not a data quality issue.
{% enddocs %}

{% docs pii_person_name %}
**PII, person name.** Direct identifier. Carries the `pii.person_name` policy tag, so principals
without `Fine-Grained Reader` on that tag see the masked value (SHA256 hash) rather than the name.
{% enddocs %}

{% docs pii_date_of_birth %}
**PII, date of birth.** A KYC identity attribute and, combined with a name, a strong re-identifier.
Carries the `pii.date_of_birth` policy tag; masked principals see `NULL` rather than a hash, because a
hashed DOB is trivially reversible (there are only ~40k plausible values).
{% enddocs %}

{% docs pii_contact %}
**PII, contact detail.** Email address. Carries the `pii.contact` policy tag; masked principals see
`SHA256`, which preserves joinability and distinct-count analytics without exposing the address.
{% enddocs %}

{% docs trading_region %}
**Trading region.** The regulated entity that owns the client relationship, derived from the client's
country by the `trading_region()` macro: `UK` (FCA), `EMEA` (BaFin), `APAC` (ASIC / MAS),
`US` (SEC / CFTC).

This is the **row access policy key**: an analyst in the UK desk group can only read rows where
`trading_region = 'UK'`. Because the divisions are separately regulated legal entities, cross-border
access to client rows is a regulatory question rather than a preference, which is what makes
row-level security the right control here rather than splitting the table N ways.
{% enddocs %}
