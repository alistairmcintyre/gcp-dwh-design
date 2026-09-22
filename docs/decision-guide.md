# Decision guide

The choices in this repo written as "if this, then that", with what each option costs. Most
branches come from something built or measured here, and link to where the detail lives. The few
that don't are marked.

1. [Erasure requests](#1-erasure-requests)
2. [Schema evolution into BigQuery](#2-schema-evolution-into-bigquery)
3. [Access control](#3-access-control)
4. [dbt across many teams](#4-dbt-across-many-teams)
5. [Orchestration](#5-orchestration)
6. [Kafka ingestion](#6-kafka-ingestion)
7. [Spark or dbt](#7-spark-or-dbt)
8. [Data contracts](#8-data-contracts)
9. [Lineage](#9-lineage)

---

## 1. Erasure requests

Detail: [`gdpr-erasure.md`](gdpr-erasure.md). Code: [`privacy/`](../privacy).

### Where does the person's data live?

```
Kafka topic
├── keyed by the person
│   ├── can be compacted ................ tombstone it, with max.compaction.lag.ms set
│   └── needs time-based retention ...... encrypt the personal fields, destroy the key
└── keyed by something else (trade, order)
    └── a tombstone can't target a person, so encrypt at the producer and destroy the key

Warehouse table
├── the row exists only because the person does ...... DELETE it
├── the row has to be kept by law (trades, AML) ...... keep it, name the legal basis,
│                                                      remove the link to the person
└── an aggregate with no person column ............... leave it, and decide separately
                                                       whether totals get restated

Files in object storage (Parquet on S3 or GCS)
├── Iceberg, Delta or Hudi table
│   └── DELETE is not enough: rewrite the files, then expire the old snapshots
├── plain Parquet
│   ├── bucketed by person ...... rewrite the files for their bucket
│   └── not ..................... find the files first, then rewrite each one
└── bucket has versioning or replication
    └── expire noncurrent versions, in every replica too

Somewhere you can't edit (backups, partner extracts, old log segments, Object Lock)
└── crypto shredding is the only option, and it only works if the data was
    encrypted before it got there
```

### Files in object storage

Measured on a real Iceberg table by reading the Parquet files directly
([`gdpr-erasure.md`](gdpr-erasure.md#files-in-object-storage-parquet-and-iceberg-on-s3)).

| If | Do | Because | What it costs |
|---|---|---|---|
| you ran `DELETE` on an Iceberg table | also `rewrite_data_files` and `expire_snapshots` | the row stays in a file on disk after `DELETE`, in both write modes | a Spark job, and time travel before the cutoff is lost |
| delete files are left pointing at rewritten data | `rewrite_position_delete_files` | the dangling-delete options didn't clear them | one more procedure per run |
| you pass a timestamp to a procedure | convert it to the session's time zone first | in summer a UTC time on a London session expires nothing, and still reports success | nothing |
| files from failed writes might hold the data | run `remove_orphan_files` on a schedule | it refuses a window under 24 hours | a day or more on the timeline |
| a column holds personal data | set its metrics mode to `none` | manifests store each file's min and max, which can be an email | no file skipping on that column |
| a table carries tags or branches | keep them short-lived | `expire_snapshots` won't remove what they pin | less time travel |
| erasure must not rewrite the whole table | partition by `bucket(N, client_id)` | one person's rows sit in one bucket | a partition scheme chosen for erasure, not only for queries |
| the S3 bucket is versioned | expire noncurrent versions after a few days | the deleted file becomes an old version, kept forever by default | less time to recover from mistakes |
| the bucket replicates | apply the same rule on every replica | deleting a specific version doesn't replicate | a rule to keep in step |
| the bucket uses Object Lock (compliance) | encrypt per person before writing | nobody can delete, root included | a key vault |
| you're relying on SSE-KMS | don't, for this | one key for the bucket erases everyone at once | per-person encryption in the app |

### Then, for the process around it

| If | Do | Because | What it costs |
|---|---|---|---|
| the job might crash halfway | destroy the key **before** deleting rows | a crash then leaves unreadable data, not readable data | nothing |
| a topic's compaction is on defaults | set `max.compaction.lag.ms` | the active segment never compacts and the cleaner waits for a 50% dirty log, so a quiet topic can hold a tombstone for weeks | more cleaner work on the broker |
| a consumer can lag longer than `delete.retention.ms` (24h default) | drive erasure from a request table, not from the log | it will never see the tombstone | a table every system can read |
| the request has to take effect today, but deleting takes time | filter the person out of staging now, delete in the nightly sweep | the law asks you to stop *using* the data straight away (Article 18) | two paths to keep in step |
| facts are built from records you're obliged to keep | filter wherever per-person rows are **derived**, not only where the person is stored | otherwise the next full build brings them back | the filter lives in more than one model |
| something downstream has to match on the field | deterministic encryption, keyed per person | joins keep working | reveals which of that person's rows share a value |
| nothing matches on it | randomised encryption | reveals nothing | can't join or group on it |
| millions of people | one data key each, in a vault, wrapped by a KMS key | KMS destroys on a schedule (30 days default, 24h minimum) and per-key cost adds up | you run the vault, and it must never be backed up |
| decryption has to happen in SQL, on BigQuery (not built here) | BigQuery AEAD functions, with keysets in a table | the roles allowed can decrypt in a query | plaintext has already reached the warehouse |

### Checking it worked

| If | Do |
|---|---|
| you want to know the sweep did its job | re-query every target afterwards; don't trust its own delete counts |
| someone could restore a backup or add a table later | re-check old requests on a schedule (`verify-all`), and keep an inventory of every table that holds a person |
| a model could put a person back | a dbt test that fails the build if an erased person appears in Gold |

### What no branch solves

- **A trained model has already learned from the data.** Deleting the feature row stops future
  scoring. The realistic fix is retraining on a schedule, so erased people age out.
- **Third parties.** Anything already sent to a CDP or ad platform needs its own deletion call.
  Encryption protects their copy only if what you sent them was ciphertext.
- **Ciphertext is pseudonymised data until the key is gone for good.** So the key destruction has
  to be irreversible, and real deletion should happen everywhere it's possible.

---

## 2. Schema evolution into BigQuery

Detail and measurements: [`beam/README.md`](../beam/README.md).

### A producer is adding a field. How are you writing to BigQuery?

```
Storage Write API (the current default)
├── withAutoSchemaUpdate on
│   ├── column added before the producer ships ...... measured: 0 blank rows of 30,000
│   └── column added as the producer ships .......... measured: 1,577 blank rows over 15.8s
└── withAutoSchemaUpdate off
    └── every row with the new field fails until the pipeline restarts, measured 30,000 of 30,000

Legacy streaming inserts (insertAll)
└── BigQuery rejects the row and Beam retries it until the schema propagates,
    usually within a few minutes

File loads
└── ALLOW_FIELD_ADDITION lets the load job add the column itself, as long as
    the schema it's given includes the new field
```

Beam only learns about a new column from BigQuery's reply to a write, and `withAutoSchemaUpdate`
forces `ignoreUnknownValues` on. So the gap never fails loudly; the new field just lands blank.

| If | Do | What it costs |
|---|---|---|
| you want the column in place before the producer ships | run the DDL from the producer's release: register the schema, add the column, then deploy | a step in every producer's pipeline; nothing here watches the registry for you |
| you can't accept a single blank value | add the column ahead, turn on auto-update, add a column listing which fields each message carried, and backfill blanks from the raw table | an extra column and a repair job |
| a few seconds of blanks are acceptable | auto-update on its own | a short gap nobody is told about unless you check |
| you're on Beam older than 2.63 and Dataflow autoscales | upgrade | nothing; older versions dropped new fields on write streams opened after the change |
| the source is Pub/Sub with schemas | commit the new schema revision ahead too | measured: Pub/Sub rejected new-version messages for up to 40s after the commit |
| a topic is quiet while the column is added (not measured here) | expect its first batch after the change may land blank | Beam learns about a column from a write reply, so with no writes it hasn't learned yet |

### Traps hit while building the tests

| If | Do |
|---|---|
| the DirectRunner never gets past `run()` on a Kafka source | `--blockOnRun=false`; it blocks on unbounded sources by default |
| Kafka consumer lag always reads empty | `commitOffsetsInFinalize()`; KafkaIO doesn't commit otherwise |
| rows rejected by the Storage Write API disappear | consume `getFailedStorageApiInserts()` and write them somewhere |
| you assumed the new schema's id is 2 | look it up; registry ids are global and reused for identical schemas |
| Dataflow fails at runtime with missing methods | use Beam's GCP BOM, and match Avro to Beam (1.12.0 for 2.76) |
| launching on Dataflow fails on a missing `hamcrest` class | add it as a runtime dependency; the options factory loads a class that needs it |

---

## 3. Access control

Detail: [`governance.md`](governance.md).

### What are you protecting?

```
a whole domain, from a team ................ dataset IAM
a curated subset of a table ................ authorized view
some rows of a table ....................... row access policy
some columns
├── the team has no need for them .......... policy tag, no grant: they are denied
└── the team needs a usable version ........ dynamic masking (needs a Google Cloud organisation)
```

Pick the coarsest option that works. Each step down the list costs more to run.

| If | Do | Because |
|---|---|---|
| the project isn't in an organisation | policy tags will deny, but can't mask | the Data Policy API refuses outside an org |
| dbt rebuilds the table (`--full-refresh`, table materialisation) | apply row policies in a dbt post-hook, not Terraform | `CREATE OR REPLACE TABLE` drops every row policy on the table |
| you're the project owner | give yourself an explicit all-regions grant | there's no owner bypass; anyone not in a policy sees zero rows |
| Gold is masked but Bronze isn't locked down | lock Bronze down first | otherwise the masking is bypassed by reading the raw table |
| a team has no business need for the field | deny rather than mask | masking hides the fact that they reached for personal data at all |
| a team needs to join on an email | `SHA256` masking | deterministic, so joins and `COUNT(DISTINCT)` still work |
| a team needs age bands, not birthdays | `DATE_YEAR_MASK` | keeps the useful part, drops the identifying part |

---

## 4. dbt across many teams

Detail: [`modelling-across-sectors.md`](modelling-across-sectors.md).

### Marketing needs something Finance built

```
is it really a general concept that two or more teams need?
├── yes ............................ move it into core; Data Engineering owns it
└── no, it's genuinely Finance's
    ├── Finance is happy to share it ... Finance makes that one model public, with a contract
    └── it's Finance's working table ... no; it stays private
```

| If | Do | What it costs |
|---|---|---|
| teams need to build their own models safely | dbt groups, with staging private to core | tests that reach into another group have to live in that group |
| one team's model depends on another team's | make it visible in CI, don't block it | a check to maintain |
| a team needs its own release schedule and CI | split into separate projects (dbt Mesh) | cross-project refs need dbt Cloud, plus the coordination cost |
| teams only differ by org chart | keep one project with groups | nothing |
| the core build fails | nothing downstream runs | a delayed report, instead of a report that looks fine and is wrong |
| dev and prod run on different engines (DuckDB and BigQuery) | target-aware config: `insert_overwrite` with `partition_by` on BigQuery, `delete+insert` with `unique_key` on DuckDB | one set of models; some config is written twice |
| runs, re-runs and backfills must be safe | pass the window in from the orchestrator as `start_date` and `end_date` vars | every incremental model has to honour the window |

---

## 5. Orchestration

Code: [`airflow/`](../airflow), [`dagster/`](../dagster).

### Which orchestrator, and where does dbt run?

```
Is the warehouse mostly dbt?
├── yes, and you want lineage, per-model tests and backfills in the UI ...... Dagster
└── no, DAGs span many systems, or you want a managed GCP runtime .......... Airflow (Composer)

Where does dbt itself run?
├── installed alongside the scheduler .... no: Airflow and dbt can't resolve together (protobuf 4 vs 5/6)
└── in its own container image ........... yes: KubernetesPodOperator on Composer,
                                             the code-location image on Dagster

How does CI push that image?
├── Workload Identity Federation ......... yes, keyless
└── a service account JSON key ........... only where federation isn't possible
```

| If | Do | Because | What it costs |
|---|---|---|---|
| you need to see which model failed | Dagster: one asset per model, dbt tests as asset checks | in Airflow the whole `dbt build` is one task, so one red box tells you little | another system to run |
| you need date-range backfills | Airflow: re-run over `data_interval`; Dagster: pick partitions in the UI | both pass the window to dbt as vars, so re-runs are idempotent | models must honour the window |
| CI deploys to GCP | Workload Identity Federation | no standing secret; a short-lived token per run, scoped to one repo or branch, and logged | a one-time GCP setup |
| the pod needs BigQuery | bind its Kubernetes account to a Google one (Workload Identity) | keyless at run time too; dbt's profile just uses ADC | IAM to set up |
| runs must be reproducible | pin the image by digest, not a tag | a re-run uses the exact code that ran first time | bump it on each deploy |
| the pods run on a separate GKE cluster | `GKEStartPodOperator` instead | same arguments plus a cluster and location | nothing |
| a run fails | Airflow `on_failure_callback` to Slack; Dagster run-failure sensors | an error-severity dbt test fails the run, so it alerts | warn-severity tests need their own sensor |

---

## 6. Kafka ingestion

Code: [`streaming/`](../streaming). The registry, [`topics.yaml`](../streaming/topics.yaml), is the
only file a person edits; one Bronze job per topic is generated from it.

### Which path?

```
How fresh does the data need to be?
├── seconds, or the logic is stateful or windowed .... Dataflow (KafkaIO), or Connect straight into BigQuery
├── minutes to hours, across many topics ............. Connect lands Avro on GCS, one Spark job pattern
│                                                      loads Bronze (this repo)
└── plain pass-through, no dedupe needed ............. Pub/Sub's BigQuery subscription

Where does Avro get decoded?
├── in Spark ............ open-source from_avro can't look up the registry, so the choices are ABRiS
│                         (Scala), a fixed schema (decodes wrongly and silently after a change), or a
│                         Python UDF (crosses into Python for every message)
└── in Kafka Connect .... it handles the registry itself and lands plain Avro, so Spark needs no
                          registry at all (this repo)

Do the topics come from CDC (Debezium makes one topic per table)?
└── yes ....... deletes matter as much as inserts: Bronze has to carry delete markers, not only appends
```

The path here trades seconds of latency for **replay**: Bronze can be rebuilt for any date from the
GCS offload, without asking Kafka for history it has already aged out. That's the wrong trade for
real-time topics, such as a client status feeding a marketing suppression list, which belong on
Dataflow.

| If | Do | Because | What it costs |
|---|---|---|---|
| you have hundreds of similar topics | one pattern plus a registry, not a pipeline per topic | a change to shared behaviour is one change, not hundreds | anything unusual needs a way out of the pattern |
| generated pipelines are checked in | CI fails when they're stale | a registry change shows its full effect in review | a generate step before commit |
| delivery can repeat messages | at-least-once, then dedupe on a business key | cheaper and simpler than exactly-once, and safe to replay | every topic declares a dedupe key |
| "latest state wins" | order by event time explicitly, never `dropDuplicates` | order only holds inside a partition; an old opt-out applied after a new opt-in re-consents someone | an `order_by` per topic |
| producers change schemas | BACKWARD compatibility | the warehouse replays old data constantly; FULL slows producers, NONE breaks the warehouse | producers can't remove fields freely |
| a message won't parse | send it to a dead-letter topic and watch its depth | blocking the partition stops everything; dropping it loses data quietly | an alert on DLQ depth |
| something breaks in the warehouse | keep Bronze exactly as received, with Kafka offset and partition | fix by replaying; "which offset?" answered without Kafka's retention | a little more storage |
| topics matter differently | freshness targets per topic, not one for the platform | one number is either too loose to mean anything or too expensive to meet | a number to agree per topic |

---

## 7. Spark or dbt

Code: [`spark/`](../spark).

### Should this be Spark at all?

```
Can it be SQL, on data already in BigQuery?
├── yes .............................................. dbt on BigQuery
└── no, because
    ├── the source isn't in BigQuery (files, JDBC, a Kafka offload) ..... Spark
    ├── the logic isn't SQL (ML features, binary payloads, state) ....... Spark
    ├── the output isn't a BigQuery table (Firestore, partner files) .... Spark
    └── slot or scan cost would be higher than a batch ................ Spark

Are there many similar pipelines?
├── yes ...................... one config-driven framework; each pipeline is a YAML file
└── each one is different .... a script per pipeline; forcing them into YAML gives worse code

Cluster or serverless?
├── spiky batch that should scale to zero ......................... Dataproc Serverless
└── notebooks, or jobs so frequent the ~1 minute start-up dominates .... a long-lived cluster
```

| If | Do | Because | What it costs |
|---|---|---|---|
| bad data must never be written | run the quality checks before the write, all in one pass (`df.observe`) | reporting a failure after writing means the incident already happened | reading the data twice |
| a check can't be evaluated (a NULL) | count it as a failure | `NOT NULL` is NULL, so the row silently passes otherwise | nothing |
| a check fails | report how many rows broke it, not just true or false | someone can judge how bad it is without reproducing it | nothing |
| you need the latest row per key | a window ordered by event time | `dropDuplicates` keeps an arbitrary row | nothing |
| reading from BigQuery | push the filter into the source options | only matching rows leave BigQuery | nothing |
| joining a small table | declare the broadcast | Spark's size estimates for BigQuery and JDBC are unreliable, and a missed broadcast is a full shuffle | nothing |
| writing to BigQuery | `direct` (Storage Write API) by default; `indirect` for very large writes | direct needs no staging bucket; load jobs are free and some types need them | billing on direct writes |
| writing files to GCS | partition on low-cardinality columns only | partitioning by client id makes millions of tiny files | coarser pruning |
| writing to Firestore | cap the sink's concurrency | Firestore has a per-second write limit and fails the batch past it | a slower write |
| you want alerts from job logs | log JSON to stdout | Cloud Logging turns it into fields, so metrics and alerts can use them | nothing |

---

## 8. Data contracts

Code: [`contracts/`](../contracts), [`services/contract-api/`](../services/contract-api).

A contract is more than a schema: it names the **owner**, what each field **means**, the
**guarantees** (grain, uniqueness, nullability), **SLOs** with numbers, what's **personal data**, and
who the **consumers** are. The classification uses the same `pii_class` words as dbt and the
Terraform taxonomy, so classifying a field in a contract is what gets it masked.

### A producer wants to change a contracted table

```
add an optional field ............... compatible
widen a type (INT64 to NUMERIC) ..... compatible
tighten nullability ................. compatible
add a required field ................ breaking: major version bump, and tell the consumers
remove a field ...................... breaking
narrow a type ....................... breaking
relax nullability ................... breaking
same type, new meaning .............. breaking, and nothing automated will catch it
```

The last one is why every field carries a written description and changing it needs the owner's
approval. Redefining `trading_revenue` from spread plus commission plus funding to spread plus
commission passes every automated check and quietly restates revenue.

| Layer | How | Catches |
|---|---|---|
| contract to dbt | the contract is mirrored as dbt `contract: {enforced: true}` plus tests | a build that produces something the contract doesn't describe |
| CI | a compatibility check against the version on `main` | a breaking change merged without a major version bump |
| runtime | Dataplex scans and freshness checks against the stated SLOs | a contract met on paper and broken in production |

---

## 9. Lineage

Code: [`lineage/`](../lineage), plus the Spark and Airflow wiring below.

The usual mistake is to start by picking a tool. Start with the question the lineage has to
answer instead. The question decides what kind of lineage you need, and that decides the tool.

Lineage works when it's a by-product of running the pipelines, recorded by the engines themselves.
It fails when it's a documentation project, because a hand-drawn graph is out of date by the next
release.

### Start here: what question does it have to answer?

```
"What breaks if I change this column?"                       impact analysis
└── column-level lineage, recorded automatically
    ├── GCP ............ Knowledge Catalog: BigQuery column lineage, including everything dbt runs
    ├── AWS ............ SageMaker Catalog
    ├── open source .... OpenLineage into DataHub or OpenMetadata, or sqlglot on your own SQL
    └── in this repo ... lineage/: trace and impact

"Where did this number come from?"                           debugging a run
└── run-level lineage: which job, which run, which inputs, when
    ├── GCP ............ Knowledge Catalog processes and runs (kept 30 days)
    ├── AWS ............ SageMaker Catalog lineage events
    ├── open source .... OpenLineage into Marquez
    └── in this repo ... dbt-ol and the Spark listener, linked to the Airflow run that started them

"Who owns this, and what does it mean?"                      governance
└── a catalog and business glossary, curated by people
    ├── commercial ..... Collibra, Atlan, Alation
    ├── open source .... DataHub, OpenMetadata
    └── when ........... after the technical lineage exists, sitting on top of it
```

The first two are technical, and should cost nobody any effort once the engines report them. The
third is governance work. That's where Collibra belongs: stewardship workflows, glossaries and
approvals, built for governance teams, with the technical lineage harvested in from elsewhere.
Starting a lineage effort there, before the engines report anything, is why it tends to feel heavy.

### On GCP

Knowledge Catalog, which was Dataplex Universal Catalog until 10 April 2026. The API, CLI and IAM
names didn't change.

```
BigQuery, including everything dbt runs .......... automatic, table and column level
Managed Service for Apache Spark (Dataproc) ...... spark.dataproc.lineage.enabled=true on the batch
Managed Service for Apache Airflow (Composer) .... turn on its lineage integration
Dataflow, Data Fusion, Iceberg REST catalog
  tables, Vertex AI pipelines .................... automatic
anything else ................................... send OpenLineage events to its API
```

| Limit | What it means |
|---|---|
| lineage is kept for **30 days** | fine for "what breaks", not an audit history; keep your own copy of the events if you need one |
| no column lineage for BigQuery load jobs or routines | a stored procedure is a gap in the graph |
| no column lineage when one job creates over 1,500 column links | very wide tables drop to table level |
| top-level columns only | fields inside a STRUCT aren't traced |
| no CMEK for the lineage metadata | matters where every store must use your own keys |

### On AWS

Amazon SageMaker Catalog, in SageMaker Unified Studio and built on DataZone. Lineage has been GA
since December 2024 and is based on OpenLineage.

```
AWS Glue and Amazon Redshift ...... captured automatically
Spark on EMR ...................... OpenLineage libraries built in
Airflow (MWAA), dbt, anything else  OpenLineage events, sent with the amazon_datazone transport
```

### Across clouds, or open source

```
Everything on one cloud? ..... that cloud's catalog is enough
Two clouds, or on-prem too? .. each native tool stops at its own edge, so send OpenLineage from
                               everything to one backend. Which backend is then just config:
                                 gcplineage       Knowledge Catalog
                                 amazon_datazone  SageMaker Catalog
                                 http             Marquez, DataHub, OpenMetadata
                                 composite        several of these at once
```

| Tool | Good at | Watch out for |
|---|---|---|
| OpenLineage | the standard format; Spark, Airflow, dbt and Flink integrations | a format only, it needs a backend |
| Marquez | the simplest backend: runs and lineage, with a UI | no catalog |
| DataHub | catalog plus column lineage; connectors for BigQuery, dbt, Airflow, Kafka, S3, Glue, Redshift | several services to run, or pay for hosting |
| OpenMetadata | catalog, lineage and quality; simpler to run than DataHub | smaller ecosystem |
| Spline | very detailed Spark lineage | Spark only |
| dbt docs | the model graph, free | table level, dbt only |
| Dagster | asset lineage as a side effect of orchestration | only what Dagster runs |

Kafka is the weak spot everywhere. Confluent Cloud has Stream Lineage; otherwise producers and
consumers have to report their own OpenLineage events.

### What's wired up here

| Piece | How | Proven by |
|---|---|---|
| column lineage for the dbt project | worked out from compiled SQL with sqlglot; ephemeral models are already inlined | `lineage/tests`, and CI on every push |
| personal-data reach | follows every `pii_class` column downstream | CI fails if it reaches a table missing from the erasure inventory, or a served column with no tag |
| Spark jobs | `spark.dataproc.lineage.enabled` in `submit.sh`; the OpenLineage listener elsewhere | `spark/tests/test_lineage_events.py` checks the events carry column lineage |
| dbt under Airflow | the pod runs `dbt-ol`, with the Airflow run passed in as its parent | a local run produced DAG, task, dbt run, then each model, in one graph |
| Dagster | its own asset graph | `openlineage-dagster` stopped being released alongside the rest of OpenLineage in October 2025, so it isn't used |

```bash
make lineage-check                                          # the checks CI runs
uv run python -m lineage.cli trace  marts.dim_client.email  # where it came from
uv run python -m lineage.cli impact raw.clients.email       # what it feeds
```

### Then, for the decisions around it

| If | Do | Because | What it costs |
|---|---|---|---|
| people need to know what a change breaks | column-level lineage | table lineage says `dim_client` depends on `stg_clients`, which is always true and answers nothing | parsing SQL, or engines that report columns |
| a column has been renamed on the way through | lineage from the SQL, not from matching names | `lower(email) as contact_email` is the same data under a new name | nothing |
| a column can't be traced | fail the check | treating it as clean is the one mistake the check exists to prevent | the odd false alarm |
| personal data is classified | tag it at the source, and let lineage carry it | tags added by hand on Gold miss every copy nobody thought of | tags on source columns |
| lineage has to cover more than one cloud | OpenLineage everywhere, one backend | native catalogs stop at their own cloud | a backend to run, or pay for |
| lineage has to be kept longer than 30 days | keep your own copy of the events (a composite transport) | Knowledge Catalog keeps 30 days | storage |
| the project is in Europe | set the lineage `location` | the transport defaults to `us-central1` | nothing |
| dbt runs in BigQuery | you already have its table and column lineage | BigQuery records it for every job; `dbt-ol` adds which model and which Airflow task | nothing |
| lineage comes from `dbt-ol` | expect ephemeral models to appear as datasets | it reports them even though no table exists | a slightly noisier graph |

