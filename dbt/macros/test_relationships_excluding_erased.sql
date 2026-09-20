{#-
    A relationships test that tolerates the one orphan erasure is allowed to create.

    A client who has asked to be erased leaves `stg_clients` on the next build, because processing
    has to stop when the request arrives. Their trades and account transactions stay: those are kept
    under a record-keeping obligation that outlives the request, as set out in
    privacy/erasure_targets.yaml. So the child rows outlive the parent, and a plain relationships
    test fails for exactly the subjects the law says to keep.

    The alternatives were worse. Dropping the test hides every real orphan, and deleting the retained
    rows to keep it green breaks the retention obligation to satisfy a data test.

    This is a generic test rather than a `where:` config because dbt's generic test config parser
    resolves neither custom macros nor `source()`.
-#}
{% test relationships_excluding_erased(model, column_name, to, field) %}

with parent as (
    select {{ field }} as id from {{ to }}
),

child as (
    select {{ column_name }} as id
    from {{ model }}
    where {{ column_name }} is not null
      and {{ column_name }} not in (
          select client_id from {{ source('raw', 'erasure_requests') }}
      )
)

select child.id
from child
left join parent on child.id = parent.id
where parent.id is null

{% endtest %}
