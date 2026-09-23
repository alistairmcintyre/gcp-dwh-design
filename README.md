# gcp-dwh-design

A runnable reference for a governed data warehouse on GCP: a dbt project on BigQuery, orchestrated
two ways (Cloud Composer and Dagster), with row and column level access control, Dataplex quality
scans, a config-driven Spark framework, a Kafka ingestion pattern for a large topic estate, data
contracts checked in CI, GDPR erasure across Kafka, the warehouse and an Iceberg lake, column-level
lineage worked out from the SQL, and a Beam/Dataflow module that measures what happens to BigQuery
when an Avro schema gains a field.

The data is synthetic and the business is invented. It's modelled on a retail trading platform
(clients, orders, trades, quotes, client money) with mobile attribution events on top, because that
shape exercises the interesting problems: regulated entities in different regions, personal data
that has to be masked per role, decimals that must never go through a float, and late-arriving
events. It isn't based on, affiliated with or derived from any company's systems, and no real data
appears anywhere in it. `scripts/generate_test_data.py` makes every row.

**The trade-offs behind all of it are in [`docs/decision-guide.md`](docs/decision-guide.md)**, as
if/then trees with what each option costs. Start there if you want the reasoning rather than the code.

## What's here

| Area | Code | Reasoning |
|---|---|---|
| dbt project: staging, marts, tests, contracts, two targets | `dbt/` | [guide §4](docs/decision-guide.md#4-dbt-across-many-teams), [modelling across teams](docs/modelling-across-sectors.md) |
| Access control: policy tags, masking, row policies, personas | `terraform/` | [guide §3](docs/decision-guide.md#3-access-control), [governance](docs/governance.md) |
| GDPR erasure: crypto shredding, tombstones, the lake | `privacy/` | [guide §1](docs/decision-guide.md#1-erasure-requests), [erasure](docs/gdpr-erasure.md) |
| Column lineage from the SQL, the personal-data checks on it, and a local Marquez UI | `lineage/` | [guide §9](docs/decision-guide.md#9-lineage) |
| Schema evolution into BigQuery, load tested | `beam/` | [guide §2](docs/decision-guide.md#2-schema-evolution-into-bigquery) |
| Orchestration: Airflow on Composer, and Dagster | `airflow/`, `dagster/` | [guide §5](docs/decision-guide.md#5-orchestration) |
| Kafka topic registry and generated Bronze jobs | `streaming/` | [guide §6](docs/decision-guide.md#6-kafka-ingestion) |
| Config-driven Spark on Dataproc Serverless | `spark/` | [guide §7](docs/decision-guide.md#7-spark-or-dbt) |
| Data contracts and the compatibility gate | `contracts/`, `services/contract-api/` | [guide §8](docs/decision-guide.md#8-data-contracts) |
| Dataplex quality scans, monitoring as code | `terraform/modules/` | [governance](docs/governance.md) |

### What has been run, and what hasn't

A repo full of YAML proves nothing on its own, so:

- **Run against a real GCP project:** the governance stack including native dynamic masking, checked
  with 25 assertions made as each persona; the Dataplex scans; the Beam load tests, one of them on
  deployed Dataflow.
- **Run locally:** the dbt project on DuckDB; the Spark framework, whose tests start a real Spark
  session; the erasure sweep, including a real Iceberg table checked file by file; column lineage
  and OpenLineage events from dbt and Spark; the contract checks; both orchestrator UIs.
- **Designed, not deployed:** the Kafka estate. There's no cluster here beyond the local one the Beam
  tests use, and no S3 bucket; the lake tests use a local Iceberg warehouse.

## Quickstart

Needs [`uv`](https://docs.astral.sh/uv/). Python is pinned to 3.12 because dbt doesn't support 3.14
yet.

```bash
uv sync
make deps data build    # dbt packages, synthetic data into DuckDB, then dbt build
make docs               # dbt docs at http://localhost:8080
make verify             # the lot, plus lint
```

The two orchestrator UIs are separate Docker Compose profiles; run one at a time:

```bash
docker compose --profile dagster up --build   # http://localhost:3000, then "Materialize all"
docker compose --profile airflow up --build   # http://localhost:8080 (admin/admin), trigger dbt_local_demo
```

On GCP, put your project and members in `terraform/envs/dev/local.auto.tfvars` (git ignores it), then:

```bash
gcloud auth application-default login
make verify-cloud   # terraform apply, load Bronze, dbt build on BigQuery, then check as each persona
```

That applies Terraform, loads Bronze, builds dbt on BigQuery, applies Terraform again (the Dataplex
scans can't be created until dbt has made the Gold tables), then checks as each persona and runs the
scans. Each step is its own `make` target too.

## Erasure requests (GDPR)

"Delete everything you hold about me" is hard because the data has been copied: into Kafka logs,
Bronze, models built on Bronze, feature tables, a lake on object storage, backups, and sometimes to a
third party. **Kafka tombstones** clear the log for topics keyed by the person. **Crypto shredding**
covers what can't be rewritten: each person's fields are encrypted with their own key before
reaching Kafka, and erasing them means destroying that key.

A request lands in `raw.erasure_requests`. The next dbt build stops processing the person, and a
nightly sweep deletes them from the warehouse and tombstones the topics, then checks its own work.
Lake tables need Spark to rewrite their files; `privacy/lakehouse.py` does that and its tests read
the Parquet files to prove it, though no scheduled lake job is wired up here.

| Decision | Why | Trade-off |
|---|---|---|
| Tombstone only topics keyed by the person and compacted | a tombstone deletes by key, and a trade topic isn't keyed by person | everything else needs encrypting at the producer |
| Pin compaction delay per topic in the registry, checked in CI | on defaults, a quiet topic can hold a tombstone for weeks | more broker cleaner work |
| Encrypt personal fields with one key per person | destroying the key reaches backups and extracts that can't be edited | a key vault that must never be backed up |
| Keys in a vault, wrapped by KMS, not one KMS key per person | KMS destroys on a schedule, and per-key cost adds up | the vault is ours to run |
| Destroy the key **before** deleting rows | a crash halfway leaves unreadable data rather than readable | none |
| Filter the person out of staging on request, delete nightly | stopping processing is immediate (Article 18); deleting everywhere isn't | two paths to keep in step |
| Also filter where per-person rows are derived | retained trades rebuilt an erased client on the next full build, caught in CI | the filter lives in two models |
| Keep trades and AML records, drop the link to the person | a legal obligation outlives the request (Article 17(3)(b)) | orphaned references, tested with an explicit exception |
| In the lake, rewrite files and expire snapshots, not just `DELETE` | measured: after `DELETE` the row is still in a Parquet file, in both write modes | a Spark job, and less time travel |
| Inventory of every table holding a person | "are you sure that's all of it?" needs an answer that isn't a grep | a file to keep up to date |
| Re-query after deleting; a dbt test asserts absence | the sweep reported success both times the rebuild bug happened | extra queries every night |

```bash
uv run python -m privacy.cli sweep --dry-run   # what the queue would touch
make erasure-sweep                             # process it, then prove it
make privacy-test                              # includes a real Iceberg table
```

## Schema evolution, in one table

A producer adds a field to its Avro schema. Measured at 100 messages a second, a minute of v1 then
five of v2:

| Setup | v2 rows with the field | blank | failed |
|---|---|---|---|
| column added ahead, no auto-update | 0 | 0 | 30,000 |
| auto-update, column added as v2 starts | 28,423 | 1,577 (15.8s) | 0 |
| column added ahead **and** auto-update | 30,000 | 0 | 0 |

Beam only learns about a new column from BigQuery's reply to a write. Details and the Dataflow run
are in [`beam/`](beam).

## The data model

```
sources (raw.*)                staging (views)              marts (incremental tables)
─────────────────────          ───────────────────          ──────────────────────────────
raw_appsflyer_events  ─▶  stg_appsflyer_events  ─┐
raw_clients           ─▶  stg_clients           ─┼─▶ fct_acquisition_events
raw_trades            ─▶  stg_trades            ─┤      (event_date x channel x platform)
raw_transactions      ─▶  stg_account_transactions
                                                 └─▶ int_client_daily_activity ─▶ fct_client_activity
                                                         (activity_date x client x channel)
```

- **Trading revenue** is `spread_revenue + commission + funding_charge`, the platform's revenue on a
  trade. It isn't the client's loss: `client_pnl` is carried separately and a test reconciles them,
  because mixing the two up is the classic way a trading report ends up wrong.
- **Notional traded** is `sum(notional_value)` over closed trades.
- Bronze, Silver and Gold are `raw`, `staging` and `marts`. Only Gold is readable outside Data
  Engineering, under row and column policy.

## Make targets

```
make install deps data build test freshness docs lint fix dag-test verify clean
make governance-apply governance-build governance-validate dq-report bq-data verify-cloud
make spark-test spark-validate topics-check contracts-check metrics
make privacy-test erasure-check erasure-deadlines erasure-sweep lineage-check lineage-demo
```

`make help` describes each one.

## Layout

```
dbt/                     the dbt project
terraform/               modules and the dev environment
privacy/                 erasure: key vault, crypto shredding, tombstones, the sweep, the lake
lineage/                 column lineage from compiled dbt SQL, and the checks built on it
beam/                    Beam/Dataflow schema-evolution module and load tests
spark/                   config-driven Dataproc Serverless framework and job specs
streaming/               Kafka topic registry and the job generator
contracts/               data contracts
services/contract-api/   the compatibility rules as a service
airflow/, dagster/       the two orchestrators
docs/                    decision guide, governance, erasure, modelling across teams
scripts/                 data generator, governance checks, metrics
```

CI runs SQLFluff, `dbt build` on DuckDB, the column lineage checks, DAG integrity,
`terraform validate`, the Spark tests, the registry staleness check, contract compatibility, and the
erasure tests including the Iceberg ones.
