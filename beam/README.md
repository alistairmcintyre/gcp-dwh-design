# Schema evolution into BigQuery with Beam and Dataflow

A producer adds a field to its Avro schema. Does the new value reach BigQuery without redeploying the
pipeline? This module tests that with real load, locally and on Dataflow.

## The short answer

1. Write with the **Storage Write API** and `withAutoSchemaUpdate(true)`. Beam also forces
   `ignoreUnknownValues()` on with it (still true in Beam 2.76.0).
2. **Add the BigQuery column before** producers send the new field. On Pub/Sub, commit the schema
   revision ahead too.
3. Rows are never rejected. The new field can land **blank for a few seconds** until Beam notices the
   column. Nothing is retried.
4. Catch those blanks with a check column (`present_fields`) and fill them back in from the raw
   table (`RepairDroppedFields`).

## What the tests showed

v1 for 60s, then v2 (adds `fill_price`, a nullable `decimal(20,6)`) for 300s, at 100 messages a second.

| Run | Setup | v2 rows with the field | v2 rows blank | v2 rows failed |
|---|---|---|---|---|
| A | Column added 60s ahead, no auto-update | 0 | 0 | 30,000 |
| B | Auto-update, column added as v2 starts | 28,423 | 1,577 (15.8s) | 0 |
| C | Neither | 0 | 0 | 30,000 |
| **D** | **Column added 60s ahead (v1 still flowing) + auto-update** | **30,000** | **0** | **0** |
| B on Dataflow | As B, Beam 2.76, auto-sharding, Pub/Sub | 28,083 | 780 (11.3s) | 0 |

A–D ran locally (DirectRunner, Kafka, Beam 2.60, one write stream). In every run the raw table has
every message exactly once. In B, D and on Dataflow the parsed table matches it row for row. In A and
C the v2 rows are in the `_failed` table instead. The Kafka and Pub/Sub backlog stayed flat through
every schema change.

On Dataflow, Pub/Sub also **rejected 1,137 v2 messages at publish** for up to 40 seconds after the
revision was committed: all of them for the first 5 seconds, tailing off after.

`RepairDroppedFields` on a copy of B's table fixed all 1,577 blanks, each value exact, and touched
nothing else.

## Why it behaves like this

- **Beam keeps its own copy of the table's columns.** It reads them once, when it starts writing.
- **Without auto-update that copy never changes.** Rows with a new field fail with
  `SchemaTooNarrowException` until the pipeline restarts (A, C). Beam raises this itself, before
  BigQuery sees the row.
- **With auto-update, Beam learns new columns only from BigQuery's reply to a write.** Rows already
  on their way are written without the value (B). If writes keep flowing while the column is added
  ahead, Beam has learned it before v2 arrives (D). A write stream that was idle during that gap can
  still blank its first batch.
- **Google says the Storage Write API notices "on the order of minutes".** We measured 11–16s. Plan
  for minutes.
- **Beam before 2.63.0 had a bug** (PR #33231): write streams opened after a schema change, for
  example when Dataflow scaled up, kept the old columns and dropped the new field.
- **The older streaming inserts (`insertAll`) behave differently.** BigQuery rejects the row and
  Beam retries it (default `alwaysRetry`) until it succeeds, "within a few minutes" per Google.
  That is where "cached schema, then retries" comes from.

## Where schema change turns into DDL

`BigQueryDdlReconciler.reconcile(tableId, avroSchema)` is the DDL step. It reads the table, works
out which Avro fields have no column, and writes the whole schema back with those added as NULLABLE.
BigQuery has no "apply this delta" call. `AvroTypes` and `BqType` choose the column type, for example
a decimal with scale above 9 becomes BIGNUMERIC.

In this module **only the load tests call it**, straight after registering v2:
`LoadTest.applyDdl` gets the latest schema from the registry (`SchemaRegistryOps.latestSchema`) and
reconciles. There is no standalone process watching the registry. In production that step belongs
in the producer's CI (register the schema, run the DDL, then deploy) or in a small service that polls
the registry.

## The files

| File | What it does |
|---|---|
| `BigQueryDdlReconciler`, `AvroTypes`, `BqType` | DDL step and Avro-to-BigQuery type mapping |
| `ConfluentWireFormat`, `HttpSchemaRegistryClient`, `AvroSchemaResolver`, `GenericRecordDecoder` | Decode a Kafka message by the schema id in its 5-byte prefix |
| `TableRowMapper` | Avro record to BigQuery row. Decimals stay exact, never via double |
| `LoadTest`, `LoadTestPipeline`, `LoadTestMetrics`, `SchemaRegistryOps` | Kafka runs A–D |
| `RepairDroppedFields` | Find blank values and fill them in from the raw table |
| `DataflowLoadTest`, `DataflowAutoUpdatePipeline`, `PubsubSchemaResolver` | Run B on Dataflow with Pub/Sub |

## Running it

**Unit tests** (JDK 17+):

```bash
mvn test        # 20 tests
```

**Kafka runs A–D** (local Kafka and Schema Registry, writes to `scratch.loadtest_<arm>_raw|_parsed|_failed`):

```bash
docker compose up -d --wait
GCP_PROJECT=your-project-id ARM=D mvn -B -q compile exec:java \
  -Dexec.mainClass=com.dwh.beam.LoadTest -Dexec.classpathScope=test
```

Settings: `ARM` (A, B, C, D), `RATE_PER_SEC` (100), `V1_SECONDS` (60), `GAP_SECONDS` (60, arm D),
`V2_SECONDS` (300), `DRAIN_SECONDS` (90), `BQ_DATASET` (scratch). Stop Kafka with `docker compose down`.

**Repair blanks** (Kafka tables only, since it decodes the Confluent format. Schema Registry must be
running with the schema ids those messages were written with):

```bash
GCP_PROJECT=your-project-id PARSED_TABLE=loadtest_b_parsed RAW_TABLE=loadtest_b_raw \
  mvn -B -q compile exec:java -Dexec.mainClass=com.dwh.beam.RepairDroppedFields
```

**B on Dataflow** (about 5p a run; the job cancels itself on exit and after 30 minutes regardless):

```bash
GCP_PROJECT=your-project-id ./scripts/dataflow-setup.sh       # once: API, bucket, worker service account
GCP_PROJECT=your-project-id mvn -B -q compile exec:java -Dexec.mainClass=com.dwh.beam.DataflowLoadTest
GCP_PROJECT=your-project-id ./scripts/dataflow-teardown.sh    # removes it all except the tables
```

## The check for blank values

`present_fields` lists the fields each message actually carried. It exists from day one, so it can
never be dropped itself.

```sql
select trade_id
from `your-project-id.scratch.loadtest_dataflow_parsed`   -- 780 rows
where 'fill_price' in unnest(present_fields) and fill_price is null
```

Tables made before this column existed can still be repaired: `RepairDroppedFields` then treats every
null as a suspect and decodes the raw message to decide.

## Traps hit along the way

- **The DirectRunner blocks on `run()` for an unbounded source** unless `--blockOnRun=false`, so
  nothing after it runs.
- **KafkaIO does not commit offsets** without `commitOffsetsInFinalize()`, so consumer lag reads empty.
- **Rows the Storage Write API rejects vanish** unless something consumes
  `getFailedStorageApiInserts()`.
- **Schema Registry ids are global and reused** for identical schemas across subjects. Look them up,
  never assume `2`.
- **Use Beam's GCP BOM** and match Avro to Beam (1.12.0 for 2.76). Otherwise mismatched client
  versions fail at runtime.
- **Launching on Dataflow needs `hamcrest` at runtime.** Beam's options factory loads a test options
  class that references it.
- **`bq add-iam-policy-binding` on a dataset needs allowlisting.** The setup script edits the
  dataset's access list instead.
