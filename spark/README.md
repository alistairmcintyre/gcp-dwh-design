# Config-driven Spark ETL on Dataproc Serverless

One image, one entrypoint, N pipelines. A pipeline is a YAML file describing **sources → transforms
→ quality gate → sink**; adding one is a config change reviewed by whoever owns the data, not a code
change reviewed by whoever owns the framework.

```bash
./submit.sh jobs/silver_client_activity.yaml                 # run on Dataproc Serverless
./submit.sh jobs/silver_client_activity.yaml --validate-only # CI gate, no cloud call
.venv/bin/python -m pytest tests -q                            # 18 tests, incl. a real local Spark run
```

---

## Why config-driven at all

The alternative is a PySpark file per pipeline. That is fine for five pipelines and unmanageable at
two hundred: every new source becomes a code review, an image build and a deploy, the fiftieth file
looks nothing like the first, and a fix to a shared concern (retry, logging, partition overwrite
semantics) has to be applied fifty times and will be applied forty-eight.

With a framework, the shared concerns live in one tested place and the per-pipeline surface is
declarative. The trade-offs, stated honestly:

| | Config-driven framework | A script per pipeline |
|---|---|---|
| Onboarding a source | edit YAML, CI validates, ship | new file, new review, new build |
| Fixing a shared concern | one change, everything inherits | N changes, some missed |
| Unusual requirement | needs a new operator, or the `sql` escape hatch | just write it |
| Debuggability | one more layer between you and the stack trace | the stack trace is your code |
| Who can add a pipeline | anyone who can review YAML | Spark engineers |

The framework wins where pipelines are *similar*, which is exactly the Kafka-topic-onboarding case.
It loses where every pipeline is genuinely different, and forcing those through a config schema
produces YAML that is code in a worse language. The `sql` transform is the pressure valve: anything
the operators cannot express goes there as Spark SQL, still reviewed as config, without a framework
release.

---

## Shape of a job spec

```yaml
name: silver_client_activity
sources:                      # first source starts the chain; all are registered as temp views
  - {name: trades, format: bigquery, options: {table: p.d.trades, filter: "..."}}
  - {name: users, format: bigquery, options: {table: p.d.users}}
transforms:                   # ordered; each takes the previous frame
  - {type: filter, expression: "status in ('won','lost')"}
  - {type: join, source: users, on: [client_id], broadcast: true}
  - {type: aggregate, group_by: [client_id], aggregations: {trading_revenue: "sum(spread_revenue) + sum(commission) + sum(funding_charge)"}}
quality:                      # evaluated BEFORE the write
  - {name: revenue_reconciles, expression: "abs(trading_revenue - (spread_revenue + commission + funding_charge)) < 0.01", on_failure: fail}
sink:
  {format: bigquery, mode: overwrite, partition_by: [activity_date], options: {table: p.d.out}}
```

| Piece | Connectors / operators |
|---|---|
| **Sources** | `bigquery`, `gcs` (parquet/avro/orc/json/csv), `jdbc` |
| **Transforms** | `select`, `filter`, `rename`, `cast`, `with_columns`, `deduplicate`, `aggregate`, `join`, `repartition`, `sql` |
| **Quality** | row-level predicates, `fail` or `warn` |
| **Sinks** | `bigquery`, `gcs`, `firestore` |

---

## Decisions worth defending

**The quality gate runs before the write, in one pass.** A job that writes bad data and then reports
a failed check has already caused the incident. Every check is collected in a single scan via the
**Observation API** (`df.observe`) rather than a `filter().count()` per check, which would need the
frame cached, and caching a large frame is itself the expensive part, competing with the job for
executor memory and spilling when it loses. Attaching the observation to the *write* would save the
second pass but make the metrics readable only after publishing, so two passes is the price of the
nothing-was-written guarantee.

**An unevaluable assertion is a failed assertion.** `filter(not (expr))` evaluates `NOT NULL` to
NULL and drops the row, so a predicate that could not be evaluated silently *passed* the gate.
`amount > 0` now flags rows where `amount` is null rather than waving them through.

Checks count violating rows rather than asserting a boolean, so the failure message says *how bad*
it is. That is the difference between an alert someone can triage and one they have to reproduce.

**`deduplicate` is a window, not `dropDuplicates`.** Kafka is at-least-once and ordered only within
a partition, so "latest row per key by event time" is the most common Bronze→Silver requirement.
`dropDuplicates` keeps an arbitrary row, which is almost never what anyone means.

**Predicate pushdown is explicit in the spec.** `options.filter` on a BigQuery source is pushed to
the storage read API, so only matching rows cross the wire. Reading a full table and filtering in
Spark looks identical in the code and costs an order of magnitude more.

**Broadcast joins are declared, not inferred.** Spark's size estimates for BigQuery and JDBC sources
are unreliable, so the automatic broadcast threshold cannot be counted on. A missed broadcast turns
a cheap join into a full shuffle.

**BigQuery writes default to `direct`** (Storage Write API): no staging bucket, lower latency, atomic
per stream. `indirect` (stage to GCS, then a load job) is better for very large writes, because load jobs
are free where Storage Write API throughput is billed, and it is required for some column types.

**Partitioning means different things per sink.** For BigQuery, `partition_by` sets the table's
partition field and BigQuery owns the physical layout. For GCS it is Spark's directory partitioning,
where low cardinality matters: partitioning by `client_id` produces millions of tiny files and turns
every later read into a metadata storm.

**The Firestore sink caps its own concurrency.** Firestore has a per-second write quota; an
unbounded Spark job will exceed it and fail the batch. The frame is repartitioned to the *sink's*
throughput rather than Spark's.

**Structured JSON logs.** Cloud Logging parses JSON on stdout into fields, which is what makes rows
written and step duration available as log-based metrics and therefore as Cloud Monitoring alerts.
Unstructured prints cannot be alerted on.

**`run()` takes an optional session.** It only stops sessions it created. Stopping one you were
handed tears down the caller's context, a real bug the end-to-end tests caught.

---

## When to use this instead of dbt + BigQuery

Most warehouse transformation should be dbt on BigQuery: cheaper, more testable, readable by
analysts, and it keeps lineage in one graph. Reach for Spark when at least one of these holds:

- the source is **not in BigQuery** (GCS files, JDBC, a Kafka offload) and loading it first costs
  more than processing it where it lies
- the transformation is **not expressible in SQL**: custom Python/Scala, ML featurisation, complex
  stateful logic, parsing binary or deeply nested payloads
- the output is **not a BigQuery table**: Firestore for online serving, files for a partner feed
- volume makes slot contention or bytes-scanned cost worse than a batch

`jobs/serving_client_margin.yaml` is the honest case: the output is a key-value document read on
the product's hot path, which BigQuery cannot serve at that latency or price.

---

## Why Dataproc Serverless rather than a cluster

No cluster to size, patch, autoscale or forget to delete, and an idle cluster nobody owns is the usual
source of a surprising Dataproc bill. Runtime version is per batch, so upgrading Spark is a job-level
decision rather than a fleet migration. Billing is per batch with scale-to-zero between runs, which
suits a warehouse's spiky batch profile. A long-lived cluster still wins for interactive notebook
work and for very high job frequency, where per-batch start-up (roughly a minute) dominates.

---

## Layout

```
framework/config.py       job spec dataclasses + validation (fails in CI, not in a paid batch)
framework/runner.py       read -> transform -> quality gate -> write, with structured logging
framework/transforms.py   the operator registry
framework/io/readers.py   bigquery / gcs / jdbc
framework/io/writers.py   bigquery / gcs / firestore
framework/main.py         entrypoint: --config gs://... [--validate-only]
jobs/*.yaml               the pipelines
submit.sh                 gcloud dataproc batches submit, with local validation first
tests/                    config validation, transform registry, and a real local Spark run
```

Infrastructure (service account, staging bucket, IAM, optional Cloud Scheduler triggers) is in
[`../terraform/modules/dataproc`](../terraform/modules/dataproc).
