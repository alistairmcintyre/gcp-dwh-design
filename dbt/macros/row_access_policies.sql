{#
    Declarative BigQuery row-level security for dbt models.

    Why this exists
    ---------------
    A BigQuery row access policy is attached to a *table*, not to a schema definition. dbt's `table`
    materialization issues `create or replace table`, which replaces the table object and therefore
    **drops every row access policy on it**. The same happens on `dbt run --full-refresh` for an
    incremental model. Left alone, a full refresh silently removes row-level security from a
    production table -- a governance control disappearing without an alert is exactly the failure a
    regulated firm cannot have.

    Managing these from Terraform instead does not fix it: Terraform would simply see drift after
    every full refresh, and the table would sit unprotected until the next apply. Whoever recreates
    the table has to reapply the policy in the same transaction-ish window, so dbt owns it.

    Ownership split (see docs/governance.md):
      Terraform  -- taxonomy, policy tags, masking data policies, IAM bindings (slow-moving, org-wide)
      dbt        -- which columns carry which tag, and which rows each group may read (moves with the model)

    Configuration lives in the `row_access_policies` var, keyed by model name, so the policies are
    reviewable in one place rather than scattered across post-hooks.
#}

{% macro apply_row_access_policies(relation) %}
    {#- dbt renders every node's hooks during parsing as well as at run time. `run_query` is a no-op
        when `execute` is false, but `log()` is not, so without this guard a `dbt parse`/`compile`
        prints "Applied row access policy ..." for relations it never touched -- and, worse, the
        relation is not fully resolved at parse time, so the schema in those lines is wrong. Guard
        the whole body: nothing here should do anything except during an actual run. -#}
    {%- if not execute -%}
        {{ return('') }}
    {%- endif -%}

    {%- if target.type != 'bigquery' -%}
        {#- DuckDB has no row access policies; the local target runs unfiltered. -#}
        {{ return('') }}
    {%- endif -%}

    {%- set all_policies = var('row_access_policies', {}) -%}
    {%- set policies = all_policies.get(relation.identifier, []) -%}

    {%- if policies | length == 0 -%}
        {{ return('') }}
    {%- endif -%}

    {#- Declarative: wipe the existing set, then recreate from config, so a policy deleted from the
        var is actually removed from the table rather than lingering. On a freshly replaced table
        this drop is a no-op; on an incremental model it is the reconciliation step. -#}
    {% do run_query("drop all row access policies on " ~ relation) %}

    {%- for policy in policies %}
        {%- set grantees = policy.get('grantees', []) -%}
        {%- if grantees | length == 0 -%}
            {{ exceptions.raise_compiler_error(
                "row_access_policies['" ~ relation.identifier ~ "']: policy '" ~ policy.get('name')
                ~ "' has no grantees. An empty grantee list would make the table unreadable."
            ) }}
        {%- endif -%}
        {% do run_query(
            "create or replace row access policy " ~ policy['name'] ~ " on " ~ relation
            ~ " grant to (" ~ (grantees | map('tojson') | join(', ')) ~ ")"
            ~ " filter using (" ~ policy['filter'] ~ ")"
        ) %}
        {% do log("Applied row access policy '" ~ policy['name'] ~ "' to " ~ relation, info=true) %}
    {%- endfor -%}

    {{ return('') }}
{% endmacro %}
