# Kafka → BigQuery ingestion at estate scale

Two hundred topics is **one pipeline pattern and two hundred rows of configuration**. This directory
holds the registry that makes that true, plus the reasoning behind the ingestion design.

```bash
python streaming/generate_topic_jobs.py --check   # CI gate: generated specs match the registry
python streaming/generate_topic_jobs.py --write   # regenerate after a registry change
```

`topics.yaml` → `spark/jobs/generated/*.yaml` → Dataproc Serverless batches.

---

## The actual problem

The engineering problem at 200 topics is not moving bytes, since every option below moves bytes fine. It
is **operability**:

- can you answer "which topics are behind SLO right now?" without opening 200 dashboards?
- when the DLQ policy changes, is that one change or 200?
- is onboarding a topic gated on the *ingestion team's* sprint capacity, or on the producing team
  being ready?
- when a schema changes at 2am, does the warehouse break, or does one topic degrade visibly?

Every design decision below is chosen to keep those four answers good. A per-topic pipeline gets the
first topic live fastest and fails all four by topic fifty.

## The registry is the interface

`topics.yaml` is the only artefact a human writes. It is simultaneously:

- the **ingestion config** (schema subject, format, dedupe key, ordering, Bronze table)
- the **data contract** (owner, compatibility mode, delivery semantics, freshness SLO)
- the **PII classification**, using the same `pii_class` vocabulary as the dbt models and the
  Terraform taxonomy, so a column is classified once, where it enters the estate, and every layer
  downstream inherits it

Adding a topic is a pull request against one file: the ingestion team reviews the contract, the
producing team reviews the semantics, CI validates it, and the generator produces the pipeline. No
new code, no new DAG, no new image.

Generated specs are checked in and CI fails if they are stale (`--check`). Checking in generated
output makes the diff of a registry change show its full blast radius in review. The alternative,
generating at deploy time, hides it.

---

## Choosing the ingestion path

| Option | How it works | Good | Bad | Use when |
|---|---|---|---|---|
| **Kafka Connect BigQuery Sink** | Connect cluster streams straight into BigQuery via Storage Write API | Lowest latency; mature; schema-registry aware; per-topic config | You now operate a Connect cluster; per-topic connector sprawl; backfill/replay is awkward | You already run Connect and need seconds-level freshness |
| **Connect → GCS → Spark → BigQuery** *(this repo)* | Connect offloads Avro to date-partitioned GCS; a Spark batch lands Bronze | Cheap; replayable from the offload; one pattern for all topics; dedupe/quality gate in one place; decouples ingest from warehouse availability | Minutes-to-hours latency, not seconds | Batch-analytical consumers, large estates, cost matters |
| **Pub/Sub Kafka connector → BQ subscription** | Bridge to Pub/Sub, then Pub/Sub's native BigQuery subscription | No processing code at all; fully managed | Little room for dedupe or transformation; another hop; schema handling is basic | Simple pass-through topics |
| **Dataflow `KafkaIO`** | Beam pipeline reads Kafka directly | Genuine streaming semantics: event-time windows, watermarks, exactly-once, stateful processing | Most expensive to run and to staff; overkill for landing raw events | The transform is genuinely stateful or windowed |

**The choice here is the second row**, and the reason is replay. A Bronze layer whose source of
truth is a GCS offload can be rebuilt for any date without asking Kafka for history it has already
aged out, and without asking the producing team for anything at all. That property is worth minutes
of latency for analytical consumers, and topics that genuinely need seconds are not competing with
this path, they should be on Dataflow.

That is also the honest limitation to state up front: **this path is wrong for real-time.** A
client-status event feeding a live marketing suppression list belongs on Dataflow or Connect, not
here. The registry's `critical: true` flag marks the topics where that conversation is due.

---

## Decisions worth defending

**At-least-once, deduplicate downstream.** Exactly-once through Kafka Connect is achievable and
costs throughput and operational complexity. Deduplicating on a business key in Bronze→Silver is
cheaper, easier to reason about, and idempotent under replay. Every topic therefore declares a
`dedupe_key` and an `order_by`, and validation rejects an entry without them.

**Ordering is per-partition only.** Kafka guarantees order within a partition, not across a topic.
So "latest state wins" needs an explicit event-time ordering, never `dropDuplicates`, which keeps an
arbitrary row. `marketing.preferences.v1` is the case that shows why this matters: applying an older
opt-out after a newer opt-in re-consents someone who opted out. That is a GDPR problem that reaches
a regulator, not a backlog.

**Schema Registry with BACKWARD compatibility.** New consumers must be able to read old data,
because a warehouse replays history constantly. `FULL` is stricter and slows producers down for
little warehouse benefit; `NONE` means the warehouse breaks whenever a producer feels like it.
Compatibility is declared per topic so a stricter mode is a deliberate, visible choice.

**Dead-letter by default.** A message that cannot be parsed goes to a DLQ topic rather than blocking
the partition or being dropped. DLQ depth is a monitored metric, because an unwatched DLQ is just a slower
way to lose data.

**Bronze keeps the payload as received.** No business logic in the landing job beyond dedupe and
audit columns. A modelling bug is then fixed by replaying from Bronze rather than by asking the
source system for history it may no longer hold.

**Kafka offset and partition are carried into Bronze.** The first question in any streaming incident
is "which offset did this come from". Answering it from the warehouse rather than from Kafka's
retention window is the difference between a ten-minute investigation and a lost one.

**Freshness SLO per topic, not per platform.** `client.status.v1` is 5 minutes because a
restricted or vulnerable customer appearing in a marketing audience is a reportable breach. `trade.execution.v2`
is 15 minutes at 1.2M events/day. One platform-wide number would be either uselessly loose or
ruinously expensive, and would tell an on-call engineer nothing about whether to get out of bed.

---

## What CI enforces

| Check | Catches |
|---|---|
| Registry validation | missing dedupe key (duplicate rows in Bronze), unknown PII class (untagged personal data), duplicate Bronze table (two topics overwriting each other), non-positive SLO |
| `--check` on generated specs | someone hand-edited a generated pipeline, or forgot to regenerate |
| `spark/tests` job-spec parse | a generator change that produces specs the framework cannot run |

---

---

## Where you deserialize decides your language

The common belief is that Python has no first-class Kafka story, so a Spark ingestion job must be
Scala. For **Spark** that is wrong, and the real constraint sits one step further along.

**The Kafka read is language-neutral.** `spark-sql-kafka-0-10` is a native JVM connector and
**PySpark uses the identical one** -- no cross-language expansion service, no second process, no
boundary. `spark.readStream.format("kafka")` in Python resolves to the same JVM DataSource as in
Scala. (This is where intuition from Apache Beam misleads: Beam's Python `KafkaIO` genuinely *is* a
cross-language transform backed by a Java expansion service, which is why Beam pipelines against
Kafka are usually written in Java. That reason does not transfer to Spark.)

**The Avro deserialization is not language-neutral.** Confluent's wire format is not plain Avro:

```
[ magic byte 0x00 ][ 4-byte schema id ][ Avro payload ]
```

so a consumer must strip five bytes and resolve the schema id against the registry. Open-source
Spark's `from_avro` takes a schema *string* and does **no registry lookup** -- registry-aware
`from_avro` is a Databricks extension, not something Dataproc has. That leaves:

| Approach | What it costs |
|---|---|
| **ABRiS** (`za.co.absa/abris`), the usual answer | a **Scala** library; reaching it from PySpark means py4j gymnastics or a helper JAR |
| Strip 5 bytes, pass a **static** schema to `from_avro` | fine until a producer evolves. Avro binary is *positional* with no field tags, so decoding v1 bytes with a v2 reader schema goes quietly wrong rather than failing -- and the registry's whole purpose is discarded |
| Python UDF using `confluent-kafka`'s `AvroDeserializer` | crosses the JVM-to-Python boundary **per message**; at this estate's volumes that becomes the job's dominant cost |
| A thin Scala/Java helper JAR called from PySpark | works, but you now maintain JVM code anyway -- at which point write the job in Scala |

**So move the boundary instead.** Kafka Connect is Confluent's own tooling and handles Schema
Registry as a first-class concern. Let it deserialize and land **plain Avro** on GCS; Spark then
reads plain Avro with no registry in the path at all, and the language choice returns to being about
team skills rather than being forced by a deserialization gap.

That is exactly what `spark/jobs/bronze_kafka_offload.yaml` does, and it is the main reason to
prefer this path: the registry concern sits in the tool that owns it, and the offload is replayable
besides.

## What is deliberately not here

No running Kafka cluster, Connect worker or schema registry. The repo demonstrates the *warehouse
side* of the contract. The registry, generator, CI gates and Bronze offload jobs are the parts a
data engineering team owns; the broker estate belongs to platform engineering, and the interface
between them is exactly the `topics.yaml` contract.


---

## Questions worth asking about this estate

The design above encodes assumptions. These are the ones to check rather than assert.

**Schema registry**
- Where does the registry live -- self-managed Confluent, Confluent Cloud, or the schema registry in
  Google Cloud Managed Service for Apache Kafka? The integration story differs for each.
- Where does deserialization happen: in Spark against the registry, or in Connect before it lands?
  If in Spark -- ABRiS, or something homegrown?
- Is compatibility mode *enforced at the registry*, or agreed by convention? Convention fails
  silently, and it fails at 2am.
- Is there one registry for the estate, or one per environment? Promoting a schema between
  environments is where multi-registry setups usually come apart.

**CDC -- the gap in the brief**
Neither the job spec nor the role's public description mentions Datastream or CDC anywhere, yet a
FTSE 100 firm with on-premises systems and a legacy AWS platform must be moving relational data
somehow. Worth asking directly, because the answer reshapes the ingestion picture:

- How does relational data reach BigQuery -- Debezium into Kafka, Datastream straight to BigQuery, or
  batch export to object storage?
- **A specific hypothesis to test:** 200+ topics is a very large number for hand-designed event
  streams, and table-level CDC is exactly how an estate reaches that count -- Debezium emits one
  topic per table. If a large share of those topics are CDC rather than domain events, then the
  registry, the dedupe key and the tombstone/delete semantics all matter far more than they would
  for event streams, and the Bronze pattern has to handle deletes rather than only appends.
- Is data from the legacy AWS platform already landing in S3 or GCS? If so, federation
  (BigQuery Omni / BigLake / an Iceberg REST catalog) is a migration bridge worth proposing -- with
  an agreed sunset date, so it stays a bridge rather than becoming the architecture.

**Operations**
- Is there a DLQ per topic, and who watches its depth? An unwatched DLQ is a slower way to lose data.
- Which topics genuinely need seconds rather than minutes, and are those on a different path? The
  batch offload pattern here is deliberately wrong for real-time.
- Where does the boundary sit between Data Engineering and platform engineering on the broker
  estate, and is the topic contract the interface between them?
