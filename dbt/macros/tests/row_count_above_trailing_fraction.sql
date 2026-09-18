{#
    Generic test: today's row count must be at least `fraction` of the trailing `lookback_days`
    daily average.

    WHY RELATIVE RATHER THAN A FIXED FLOOR. A static `min_rows` is right for "did anything arrive"
    and wrong for "is the volume plausible": the correct number moves with growth, seasonality and
    market hours, so a hand-set threshold is either so low it never fires or so tight it cries wolf.
    Across a 200-topic estate nobody maintains 200 static thresholds, and the ones that exist go
    stale and get ignored, which is worse than not having them.

    This compares against the source's own recent history instead, so it travels with the data.
    It is deliberately coarse; for real seasonality (weekday/weekend, market open) use Elementary's
    `volume_anomalies`, which models the baseline properly. This test exists because it is
    dependency-free, blocking, and explains itself in the failure row.

    Set `severity: warn` unless a volume drop genuinely should stop the pipeline.

    Usage:
        data_tests:
          - row_count_above_trailing_fraction:
              date_column: closed_at
              fraction: 0.4
              lookback_days: 14
              severity: warn
#}
{% test row_count_above_trailing_fraction(
       model, date_column, fraction=0.5, lookback_days=14, start_date=none
   ) %}

{%- set window_start = start_date or var('start_date') -%}

with daily as (
    select
        cast({{ date_column }} as date) as event_date,
        count(*) as row_count
    from {{ model }}
    group by 1
),

target_day as (
    select row_count from daily where event_date = date '{{ window_start }}'
),

baseline as (
    select avg(row_count) as avg_row_count
    from daily
    where event_date < date '{{ window_start }}'
        and event_date >= {{ dbt.dateadd('day', -lookback_days, "date '" ~ window_start ~ "'") }}
)

select
    t.row_count,
    b.avg_row_count,
    {{ fraction }} as required_fraction,
    '{{ window_start }}' as window_start
from target_day as t
cross join baseline as b
-- No baseline yet (first days of a new source) is not a failure; it is an absence of evidence.
where b.avg_row_count is not null
    and t.row_count < b.avg_row_count * {{ fraction }}

{% endtest %}
