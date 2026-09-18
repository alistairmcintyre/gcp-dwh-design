{%- set money = 'numeric' if target.type == 'bigquery' else 'decimal(18,2)' -%}
{{
    config(
        materialized='table',
        schema='features',
        cluster_by=['client_id'] if target.type == 'bigquery' else none
    )
}}

-- ML feature table: one row per client, shaped for Vertex AI Feature Store.
--
-- WHY THIS IS A DATA ENGINEERING ARTEFACT, NOT A DATA SCIENCE ONE
-- Training/serving skew -- a model trained on one definition of a feature and served another -- is
-- the classic way an ML system fails silently in production. It is a *data* problem, not a modelling
-- problem, which is why the feature table belongs in the warehouse, in the same dbt project, under
-- the same tests and contracts as everything else. Vertex AI Feature Store then syncs THIS table to
-- its online store, so the training path (read BigQuery) and the serving path (read the online
-- store) resolve to one definition maintained in one place.
--
-- SHAPE REQUIRED BY VERTEX AI FEATURE STORE
--   * an entity id column          -- `client_id`, declared as entity_id_columns on the Feature Group
--   * a `feature_timestamp` column -- used for point-in-time correctness when generating training
--                                     data, so a training row never sees a feature value that did
--                                     not exist yet. Getting this wrong leaks the future into the
--                                     training set and produces a model that looks excellent
--                                     offline and fails the moment it is deployed.
--   * the feature columns themselves
--
-- NOTE: deliberately no PII. Features are served to low-latency online consumers where the access
-- controls of docs/governance.md do not apply -- an online store lookup is not a BigQuery query, so
-- policy tags and row access policies are simply not in the path. Anything sensitive must be
-- aggregated or excluded HERE, at the point it leaves the governed warehouse.

with activity as (
    select
        client_id,
        activity_date,
        trade_count,
        total_notional,
        trading_revenue,
        net_deposit
    from {{ ref('fct_client_activity') }}
),

windowed as (
    select
        client_id,
        max(activity_date) as last_activity_date,
        count(distinct activity_date) as active_days_30d,
        sum(trade_count) as trade_count_30d,
        sum(total_notional) as notional_30d,
        sum(trading_revenue) as trading_revenue_30d,
        sum(net_deposit) as net_deposit_30d
    from activity
    where activity_date >= {{ dbt.dateadd('day', -30, 'current_date') }}
    group by 1
),

customer as (
    select
        client_id,
        trading_region,
        acquisition_channel,
        platform,
        -- Tenure, in days. Derived here rather than serving the raw registration date, which is a
        -- weak feature and a strong quasi-identifier.
        {{ dbt.datediff('registration_date', 'current_date', 'day') }}
            as tenure_days
    from {{ ref('dim_client') }}
)

select
    c.client_id,
    -- The point-in-time key. Set to the build time rather than the data's own max date: it records
    -- when this feature vector became *knowable*, which is what point-in-time joins need.
    {{ dbt.current_timestamp() }} as feature_timestamp,

    -- Behavioural features
    coalesce(w.active_days_30d, 0) as active_days_30d,
    coalesce(w.trade_count_30d, 0) as trade_count_30d,
    cast(coalesce(w.notional_30d, 0) as {{ money }}) as notional_30d,
    cast(coalesce(w.trading_revenue_30d, 0) as {{ money }}) as trading_revenue_30d,
    cast(coalesce(w.net_deposit_30d, 0) as {{ money }}) as net_deposit_30d,

    c.tenure_days,

    -- Low-cardinality categoricals, safe to serve
    c.trading_region,
    c.acquisition_channel,
    c.platform
from customer as c
left join windowed as w on c.client_id = w.client_id
