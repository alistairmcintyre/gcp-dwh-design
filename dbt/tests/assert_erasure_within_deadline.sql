-- No erasure request is sitting past its deadline.
--
-- Article 12(3) gives one month from the request, extendable by two in limited cases. A queue that
-- quietly grows is the most common way that deadline is missed, so the build says something while
-- there is still time to act rather than after.
--
-- Warned at 25 days rather than failed at 30, because a test that goes red on the day of the breach
-- is telling you something you can no longer fix. It becomes an error once five are backed up,
-- which is a process that has stopped working rather than one request that slipped.

{{ config(severity='warn', warn_if='>0', error_if='>=5') }}

{%- set days_open -%}
    {{ dbt.datediff('requested_at', dbt.current_timestamp(), 'day') }}
{%- endset -%}

select
    client_id,
    requested_at,
    {{ days_open }} as days_open
from {{ source('raw', 'erasure_requests') }}
where completed_at is null
  and {{ days_open }} >= 25
