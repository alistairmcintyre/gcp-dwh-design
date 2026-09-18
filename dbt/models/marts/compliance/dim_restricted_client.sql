{{
    config(
        materialized='table',
        group='compliance',
        access='private',
        schema='marts_compliance'
    )
}}

-- Compliance domain mart: the suppression list.
--
-- One row per client who must NOT receive marketing. Restriction, suspension, closure and
-- vulnerability all suppress; the reason is carried so Compliance can evidence *why* a client was
-- suppressed, which is what an audit actually asks for.
--
-- NOTE WHAT THIS MODEL REFERENCES. Only `dim_client`, which is `access: public`. It cannot reach
-- into `stg_clients` -- that model is `access: private` to the `core` group, so the ref would fail
-- at parse time. This is the platform interface being enforced rather than documented: Compliance
-- builds freely in this folder, and structurally cannot depend on Data Engineering's internals.

with clients as (
    select * from {{ ref('dim_client') }}
)

select
    client_id,
    trading_region,
    client_category,
    account_status,
    account_status_reason,
    registration_date,
    last_activity_date,
    -- Vulnerability and appropriateness failures are the two reasons that are not merely commercial:
    -- marketing to either is a reportable breach rather than a preference violation.
    account_status_reason in ('VULNERABLE_CLIENT', 'APPROPRIATENESS_FAILED') as is_regulatory_suppression
from clients
where account_status <> 'ACTIVE'
