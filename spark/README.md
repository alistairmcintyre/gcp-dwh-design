# Spark framework (Dataproc Serverless)

One image, one entrypoint, many pipelines. Each pipeline is a YAML file describing its sources,
transforms, quality checks and sink; adding one is a config change, not a code change.

| Path | What it is |
|---|---|
| `framework/` | config validation, the runner, the transforms, and the readers and writers |
| `jobs/*.yaml` | the pipelines; `jobs/generated/` is written from the Kafka topic registry |
| `submit.sh` | validates locally, then submits a Dataproc Serverless batch |
| `tests/` | config validation, the transforms, and a real local Spark run |

```bash
./submit.sh jobs/silver_client_activity.yaml                   # run on Dataproc Serverless
./submit.sh jobs/silver_client_activity.yaml --validate-only   # check it, no cloud call
make spark-test                                                # from the repo root
```

Sources: `bigquery`, `gcs`, `jdbc`. Transforms: `select`, `filter`, `rename`, `cast`,
`with_columns`, `deduplicate`, `aggregate`, `join`, `repartition`, `sql`. Sinks: `bigquery`,
`gcs`, `firestore`. Infrastructure is in `terraform/modules/dataproc`.

When to use Spark rather than dbt, and the choices inside the framework:
[decision guide, section 7](../docs/decision-guide.md#7-spark-or-dbt).
