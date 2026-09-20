# gcp-dwh-design

A runnable reference for a governed data warehouse on GCP: a dbt project on BigQuery, orchestrated
two ways (Cloud Composer and Dagster), with row and column level access control, Dataplex quality
scans, a config-driven Spark framework, a Kafka ingestion pattern for a large topic estate, data
contracts checked in CI, and a Beam/Dataflow module that measures what actually happens to BigQuery
when an Avro schema gains a field.

The data is synthetic and the business is invented. It is modelled on a retail trading platform
(clients, orders, trades, quotes, client money) with mobile attribution events on top, because that
shape exercises the interesting problems: regulated entities in different regions, PII that has to be
masked per role, decimals that must not go anywhere near a float, and late-arriving events. It is not
based on, affiliated with or derived from any company's systems, and no real data appears anywhere in
it. `scripts/generate_test_data.py` makes every row.

Two local UIs, as separate Docker Compose profiles (run one at a time):

```bash
docker compose --profile dagster up --build     # Dagster UI  -> http://localhost:3000
docker compose --profile airflow up --build     # Airflow 3 UI -> http://localhost:8080 (admin/admin)
```

## What is here

| Area | Contents |
|---|---|
| **dbt project** (`dbt/`) | staging, intermediate and incremental marts, two targets (DuckDB for dev, BigQuery for prod), tests, column docs, contracts, exposures, source freshness, Elementary |
| **Governance** (`terraform/`, [`docs/governance.md`](docs/governance.md)) | Dataplex taxonomy and policy tags, dynamic data masking, row access policies, per-persona IAM, all in Terraform, applied to a real project and checked by impersonating each persona |
| **Modelling for many teams** ([`docs/modelling-across-sectors.md`](docs/modelling-across-sectors.md)) | dbt groups and access, so one project serves Finance, Compliance, Marketing and Risk without them treading on each other, plus how the build is ordered |
| **Schema evolution** (`beam/`) | a Beam pipeline and load tests measuring what BigQuery's Storage Write API does when a producer adds a field, including a run on real Dataflow |
| **Erasure requests** (`privacy/`, [`docs/gdpr-erasure.md`](docs/gdpr-erasure.md)) | crypto shredding and Kafka tombstones: per-subject keys, an inventory of every place a subject appears, a sweep that deletes and proves it, and a dbt test that fails if an erased client comes back |
| **Orchestration, Airflow** (`airflow/`) | dbt baked into a container image and run by Composer through KubernetesPodOperator, with a Slack alert on failure |
| **Orchestration, Dagster** (`dagster/`) | the same dbt project as software-defined assets, dbt tests as asset checks, daily partitions, schedules, failure sensors |
| **Spark framework** (`spark/`) | config-driven ETL for Dataproc Serverless: one image, N pipelines from YAML, with a quality gate before the write |
| **Kafka ingestion** (`streaming/`) | a topic registry that generates one Bronze offload job per topic, with CI failing on stale generated output |
| **Data contracts** (`contracts/`, `services/contract-api/`) | contracts with owners, SLOs, PII classification and consumers, plus a FastAPI service that checks compatibility and fails CI on a breaking change |
| **Data quality** (`terraform/modules/dataplex_quality`) | Dataplex scans over Bronze and Gold, with a report that gates CI |
| **Observability** (`terraform/modules/monitoring`) | log-based metrics and alert policies as code |
| **Engineering metrics** (`scripts/dora_metrics.py`) | DORA four from git history, adapted for a data team |

### What has been run, and what has not

Worth being clear, because a repo full of YAML proves nothing on its own.

- **Run against a real GCP project:** the governance stack, including native dynamic data masking,
  with 25 assertions made while impersonating each persona. The Dataplex quality scans. The Beam
  schema-evolution load tests, one of them on deployed Dataflow.
- **Run locally:** the whole dbt project on DuckDB, the Spark framework (its tests start a real local
  Spark session), the contract compatibility checks, and both orchestrator UIs.
- **Designed, not deployed:** the Kafka estate. There is no cluster or Connect worker here beyond the
  local Kafka the Beam tests use. `streaming/` is the registry, the generator and the reasoning.

## Layers

The dbt layer names and the Bronze/Silver/Gold vocabulary mean the same thing. Both appear because
different audiences use different words for it.

| Medallion | This repo | Materialisation | Who may read it |
|---|---|---|---|
| Bronze | `raw` (sources) | landed tables, unmodelled | Data Engineering only |
| Silver | `staging` (+ `intermediate`) | views and ephemeral models, cleaned and conformed | engineers and modellers |
| Gold | `marts` | incremental tables, contracted, documented, governed | the self-serve surface, under row and column policy |

BigQuery datasets carry a `layer` label, so the tier is queryable from `INFORMATION_SCHEMA` rather
than inferred from a naming convention.

## The data model

```
sources (raw.*)                staging (views)              marts (incremental tables)
─────────────────────          ───────────────────          ──────────────────────────────
raw_appsflyer_events  ─▶  stg_appsflyer_events  ─┐
raw_clients           ─▶  stg_clients           ─┼─▶ fct_acquisition_events
raw_trades            ─▶  stg_trades            ─┤      (event_date x channel x platform:
raw_transactions      ─▶  stg_account_transactions      installs, registrations, conversion)
                                                 └─▶ int_client_daily_activity ─▶ fct_client_activity
                                                         (activity_date x client x channel:
                                                          trade_count, notional, trading revenue,
                                                          deposits, withdrawals, net_deposit)
```

The definitions that matter, also written as dbt doc blocks on the columns:

- **Notional traded** is `sum(notional_value)` over closed trades: trade size times the price of the
  underlying.
- **Trading revenue** is `spread_revenue + commission + funding_charge`, the platform's revenue on a
  trade. It is not the client's loss. `client_pnl` is carried separately and the two are reconciled
  by a singular test, because conflating them is the classic way a trading report ends up wrong.
- **Acquisition channel** groups the AppsFlyer `media_source` into something readable
  (`facebook_ads` becomes "Facebook Ads") through the `dim_channel_grouping` seed.

## Quickstart: local, on DuckDB

Needs [`uv`](https://docs.astral.sh/uv/). Python is pinned to 3.12, because dbt does not support 3.14
yet.

```bash
uv sync                       # venv + dev dependencies
cp .env.example .env          # optional, the defaults work
make deps                     # dbt deps
make data                     # synthetic raw data into data/dev.duckdb
make build                    # dbt build: seed, run and test on DuckDB
make docs                     # dbt docs at http://localhost:8080
```

`make verify` runs the lot plus lint. There is nothing special about `make` here; every target is a
thin wrapper around a `uv run` command.

```bash
uv run python -c "import duckdb; con=duckdb.connect('data/dev.duckdb'); \
  print(con.sql('select * from marts.fct_client_activity limit 5'))"
```

## Quickstart: GCP, the governed warehouse

Deploys the datasets, the PII taxonomy, the personas and the Dataplex scans, builds the dbt project
on BigQuery with policy tags and row access policies applied, then checks the controls by querying as
each persona.

```bash
gcloud auth application-default login
# put your project and members in terraform/envs/dev/local.auto.tfvars (git ignores it)

make governance-apply      # terraform: datasets, taxonomy, policy tags, personas, scans
make bq-data               # load synthetic raw data into Bronze
make governance-build      # dbt build on BigQuery with tags and row policies applied
make governance-validate   # impersonate each persona and assert what they can see
make dq-report             # run the Dataplex scans and print per-dimension scores
```

`make verify-cloud` runs all five. The write-up, including the parts that only showed up on a real
project, is in [`docs/governance.md`](docs/governance.md).

```
PERSONA: uk_desk  --  UK trading desk: no PII, UK rows only
  [PASS] Bronze (raw.clients) is not readable                403 Access Denied
  [PASS] Row access policy returns the expected regions      saw ['UK'], expected ['UK']  (UK=199)
  [PASS] Column-level: person_name is masked                 all 398 values blanked
  [PASS] Column-level: date_of_birth is masked               truncated to year, e.g. 1998-01-01
  [PASS] Column-level: contact (email) is masked             SHA256, 199 distinct of 199
```

## Schema evolution into BigQuery

A producer adds a field to its Avro schema. Does the value reach BigQuery without redeploying the
pipeline? `beam/` answers that with load tests rather than opinion: 1 minute of v1, then 5 minutes of
v2 at 100 messages a second, counting what lands.

| Run | Setup | v2 rows with the new field | v2 rows blank | failed |
|---|---|---|---|---|
| A | column added 60s ahead, no auto-update | 0 | 0 | 30,000 |
| B | auto-update, column added as v2 starts | 28,423 | 1,577 (15.8s) | 0 |
| C | neither | 0 | 0 | 30,000 |
| D | column added 60s ahead + auto-update | 30,000 | 0 | 0 |

Beam reads the table's columns once when it starts writing, and only refreshes them from BigQuery's
reply to a write, and only with `withAutoSchemaUpdate`. So adding the column early is not enough on
its own, and the flag on its own leaves the new field blank for a few seconds. Both together, with
traffic flowing, lost nothing. The same setup on deployed Dataflow behaved the same way. Details,
including the check that finds blanked values and the job that backfills them from the raw table, are
in [`beam/README.md`](beam/README.md).

## Erasure requests

"Delete everything you hold about me" is easy to say and hard to carry out, because the data has been
copied: into Kafka logs, Bronze, models built on Bronze, feature tables, backups, and sometimes out
to a third party. Two mechanisms do the work, and they cover different things.

**Kafka tombstones** clear the log. A record with the subject's key and a null value on a compacted
topic removes their history and tells every consumer to delete their copy. It only works where the
topic is keyed by the subject and compacted, and the timing has to be pinned: compaction waits for
closed segments, so on a quiet topic a tombstone can sit unapplied for weeks. Each topic declares
its method in `streaming/topics.yaml` and CI rejects one that cannot deliver what it claims.

**Crypto shredding** covers what cannot be rewritten. Each subject's personal fields are encrypted
with their own key before they reach Kafka, so erasing is destroying the key: backups, partner
extracts and records kept under a legal obligation all stop meaning anything at once. A trade has to
survive for record-keeping; the name attached to it does not.

```bash
uv run python -m privacy.cli sweep --dry-run   # what the queue would touch
make erasure-sweep                             # process it, then prove it
make erasure-check                             # every topic can satisfy a request
make erasure-deadlines                         # how long each open request has waited
```

The sweep destroys the key first (if it dies halfway, the subject is already unreadable), deletes
every row the inventory in `privacy/erasure_targets.yaml` marks `delete`, tombstones the keyed
topics, then re-queries to prove nothing is left. Meanwhile `stg_clients` drops the subject on the
next build, which is Article 18 restriction of processing and takes minutes rather than waiting for
the sweep. What it cannot do, including trained models and the pseudonymisation argument, is written
down in [`docs/gdpr-erasure.md`](docs/gdpr-erasure.md).

## Orchestration: two ways

Both run the same dbt project. dbt is the portable core and the orchestrator is a swappable layer.
Neither installs dbt in-process, because Airflow and dbt cannot resolve together (protobuf 4 against
5/6), so both run dbt in its own container: `KubernetesPodOperator` on Composer's GKE, or
`DockerOperator` and the Dagster code-location image locally.

| | Airflow (`airflow/`) | Dagster (`dagster/`) |
|---|---|---|
| Unit of work | tasks; the whole `dbt build` is one task | one asset per dbt model, one check per dbt test |
| dbt lineage | in dbt docs, separately | native: the asset graph is the dbt DAG |
| dbt tests | pass or fail inside the task log | asset checks, green or red per model per run |
| Backfills | re-triggered runs over `data_interval` | partition grid, pick a date range in the UI |
| Ingestion to dbt | separate DAGs or systems | the dev `raw_data` asset shares the dbt source keys, so one graph |
| Alerting | `on_failure_callback` to Slack | run-failure sensors plus red checks |
| Suits | a managed GCP runtime and many-system DAGs | a dbt-centric warehouse, lineage and observability |

More on each in [`airflow/README.md`](airflow/README.md) and [`dagster/README.md`](dagster/README.md).

Locally each profile is self-contained, with its own Postgres and storage:

```bash
cp .env.example .env                 # HOST_PROJECT_DIR and DOCKER_GID for the Airflow profile
make data                            # seed ./data/dev.duckdb, shared by both UIs

docker compose --profile dagster up --build     # Assets -> "Materialize all" for the full lineage
docker compose --profile airflow up --build     # trigger dbt_local_demo: dbt on DuckDB via DockerOperator
docker compose --profile dagster down -v        # stop and wipe that profile
```

The Composer DAGs parse locally but target GKE, so `dbt_local_demo` is the one that actually runs on
a laptop.

## One project, two targets

```
              ┌──────────── dev (local) ────────────┐   ┌────────── prod (Composer) ──────────┐
generate ───▶ │ DuckDB raw schema ─▶ dbt ─▶ DuckDB   │   │ BigQuery raw ─▶ dbt ─▶ BigQuery      │
test data     │ (source and destination)             │   │ (source and destination)            │
              └──────────────────────────────────────┘   └─────────────────────────────────────┘
```

The same models run on both. Portability comes from target-aware config: BigQuery uses
`insert_overwrite` with a date `partition_by`, replacing only the partitions touched, and DuckDB uses
`delete+insert` with a `unique_key`. The incremental window comes from the orchestrator's run
interval, passed to dbt as `start_date` and `end_date` vars, so any run, re-run or backfill of a date
range is idempotent.

## Make targets

```
make install   # uv sync (dev group)
make deps      # dbt deps
make data      # generate synthetic raw data into DuckDB
make build     # dbt build (TARGET=dev by default, TARGET=prod for BigQuery)
make test      # dbt test
make freshness # dbt source freshness
make docs      # dbt docs generate + serve
make lint/fix  # SQLFluff
make dag-test  # AST-compile every DAG file, no Airflow needed
make verify    # deps + data + build + freshness + lint
make clean     # remove the DuckDB file and dbt artefacts
```

Cloud and component targets: `governance-apply`, `governance-build`, `governance-validate`,
`dq-report`, `bq-data`, `verify-cloud`, `spark-test`, `spark-validate`, `topics-check`,
`contracts-check`, `privacy-test`, `erasure-check`, `erasure-deadlines`, `erasure-sweep`,
`metrics`.

## Layout

```
dbt/                     the dbt project (models, seeds, macros, tests)
terraform/               modules (governance, bigquery, dataplex_quality, dataproc, cloudrun,
                         monitoring, access_personas) and the dev environment root
beam/                    Beam/Dataflow schema-evolution module and its load tests
privacy/                 erasure: per-subject key vault, crypto shredding, tombstones, the sweep
spark/                   config-driven Dataproc Serverless ETL framework, job specs, tests
streaming/               Kafka topic registry and the Bronze offload job generator
contracts/               data contracts (owner, SLO, PII classification, consumers)
services/contract-api/   FastAPI contract registry for Cloud Run, behind API Gateway
docs/                    governance, cross-team modelling, and the erasure design
scripts/                 synthetic data generator, governance validation, metrics
airflow/                 dbt image, KubernetesPodOperator wrapper, Composer DAGs, local demo DAG
dagster/                 Dagster code location (assets, checks, partitions, jobs, schedules, sensors)
docker-compose.yml       dagster and airflow profiles for the local UIs
tests/                   DAG integrity and compile checks
.github/workflows/       CI, and the image deploy workflows (WIF or SA key)
```

## CI

GitHub Actions runs SQLFluff, `dbt build` on DuckDB, DAG integrity, `terraform fmt` and `validate`,
the Spark tests, the topic registry staleness check, contract compatibility, and the erasure tests
plus the check that every topic can satisfy an erasure request. pre-commit runs the
fast subset locally.
