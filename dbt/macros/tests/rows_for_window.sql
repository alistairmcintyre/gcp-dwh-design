{#
    Generic test: assert the source/model holds at least `min_rows` rows whose EVENT date falls in
    the run's build window.

    WHY THIS IS NOT `source freshness`. Freshness answers "is the pipe alive?" using an
    ingestion-controlled timestamp. This answers a different and more dangerous question: "does the
    partition I am about to build actually contain anything?"

    They fail independently. Ingestion can be perfectly healthy, with freshness green, while the window
    you are about to rebuild is empty, because the producing system emitted nothing for that day. The
    marts use `insert_overwrite` (BigQuery) / `delete+insert` (DuckDB), so building an empty window
    **overwrites a good partition with nothing**. That is silent data loss, and freshness will not
    catch it. This test runs against the SOURCE, before the mart is built, so `dbt build` stops first.

    Defaults come from the same `start_date`/`end_date` vars Airflow and Dagster pass as the run's
    data interval, so the assertion always matches the window actually being built.

    Usage:
        data_tests:
          - rows_for_window:
              date_column: closed_at
              min_rows: 50
#}
{% test rows_for_window(model, date_column, min_rows=1, start_date=none, end_date=none) %}

{%- set window_start = start_date or var('start_date') -%}
{%- set window_end = end_date or var('end_date') -%}

with windowed as (
    select count(*) as row_count
    from {{ model }}
    where cast({{ date_column }} as date)
        between date '{{ window_start }}' and date '{{ window_end }}'
)

select
    row_count,
    {{ min_rows }} as min_expected,
    '{{ window_start }}' as window_start,
    '{{ window_end }}' as window_end
from windowed
where row_count < {{ min_rows }}

{% endtest %}
