{#
  Use the model's custom `+schema` (staging / marts / raw / ...) directly, in every environment,
  instead of dbt's default `<target_schema>_<custom>` prefixing. This keeps schema/dataset names
  identical and predictable across DuckDB (dev) and BigQuery (prod): staging, intermediate, marts, etc.
  Models without a custom schema fall back to the target's default schema/dataset.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
