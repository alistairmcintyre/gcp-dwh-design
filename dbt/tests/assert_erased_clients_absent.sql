{{ config(group='ml') }}

-- Nobody who asked to be erased is still in the warehouse.
--
-- Grouped with `ml` rather than `core` because it reads feat_client_activity, which is private to
-- that group, and a test can only belong to one. dbt refuses to parse the project otherwise, which
-- is the access model doing its job: a test is as much a consumer as a model is.
--
-- This is the test that has to exist, because every other part of the erasure process reports on
-- itself. The sweep says how many rows it deleted, the vault says the key is gone, the connector
-- says the tombstone was produced. None of that proves the person is actually absent from the
-- tables people query, and a model added next quarter that reads an older source can put them back.
--
-- It asserts the completed requests only. A request that the sweep has not reached yet is allowed
-- to still have Gold rows: processing stops immediately through stg_clients, and the rows go when
-- the sweep runs. What is never allowed is a request marked complete whose data is still queryable.
-- Whether a pending request is taking too long is a different question, asked by
-- assert_erasure_within_deadline.sql.

with requested as (
    select
        client_id,
        completed_at
    from {{ source('raw', 'erasure_requests') }}
    where completed_at is not null
),

still_present as (
    select
        r.client_id,
        r.completed_at,
        'marts.dim_client' as found_in
    from requested as r
    inner join {{ ref('dim_client') }} as d on r.client_id = d.client_id

    union all

    select
        r.client_id,
        r.completed_at,
        'marts.fct_client_activity' as found_in
    from requested as r
    inner join {{ ref('fct_client_activity') }} as f on r.client_id = f.client_id

    union all

    select
        r.client_id,
        r.completed_at,
        'features.feat_client_activity' as found_in
    from requested as r
    inner join {{ ref('feat_client_activity') }} as x on r.client_id = x.client_id
)

select
    client_id,
    found_in,
    'erased on ' || cast(completed_at as varchar) || ' but still present' as problem
from still_present
