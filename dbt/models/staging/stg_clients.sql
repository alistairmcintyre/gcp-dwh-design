with source as (
    select * from {{ source('raw', 'clients') }}
)

select
    client_id,
    appsflyer_id,
    -- PII. Carried through unmasked at the Silver layer; column-level security is applied on the
    -- Gold dimension (`dim_client`), which is what analysts and BI query. Staging views are not
    -- granted to consumers -- see docs/governance.md for the layer-by-layer access model.
    first_name,
    last_name,
    date_of_birth,
    email,
    registration_time,
    cast(registration_time as date) as registration_date,
    country,
    {{ trading_region('country') }} as trading_region,
    -- MiFID II client categorisation. Drives leverage caps, negative balance protection and the
    -- disclosures a client is entitled to, so it is a regulatory attribute, not a marketing segment.
    client_category,
    -- Lifecycle state. `account_status_reason` carries the restriction cause -- an appropriateness
    -- failure or a vulnerability flag must reach downstream marketing suppression quickly, which is
    -- why this attribute drives a sub-minute operational path as well as the warehouse.
    account_status,
    account_status_reason,
    acquisition_media_source as media_source,
    platform,
    _loaded_at as loaded_at
from source
