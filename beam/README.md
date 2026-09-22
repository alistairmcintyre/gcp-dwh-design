# Schema evolution into BigQuery (Beam and Dataflow)

What happens to a new Avro field on its way into BigQuery through the Storage Write API, measured
under load: a minute of v1, then five minutes of v2 (which adds `fill_price`) at 100 messages a
second.

| Run | Setup | v2 rows with the field | blank | failed |
|---|---|---|---|---|
| A | column added 60s ahead, no auto-update | 0 | 0 | 30,000 |
| B | auto-update, column added as v2 starts | 28,423 | 1,577 (15.8s) | 0 |
| C | neither | 0 | 0 | 30,000 |
| D | column added 60s ahead, plus auto-update | 30,000 | 0 | 0 |
| B on Dataflow | as B, Beam 2.76, Pub/Sub | 28,083 | 780 (11.3s) | 0 |

A to D ran locally on Kafka, B again on deployed Dataflow. What it means, and what to do about it:
[decision guide, section 2](../docs/decision-guide.md#2-schema-evolution-into-bigquery).

| Path | What it is |
|---|---|
| `LoadTest`, `LoadTestPipeline`, `LoadTestMetrics` | runs A to D against local Kafka and Schema Registry |
| `DataflowLoadTest`, `DataflowAutoUpdatePipeline`, `PubsubSchemaResolver` | run B on Dataflow |
| `BigQueryDdlReconciler`, `AvroTypes`, `BqType` | adds the BigQuery column for a new Avro field |
| `GenericRecordDecoder`, `TableRowMapper`, `ConfluentWireFormat` | decodes by schema id; decimals never go through a double |
| `RepairDroppedFields` | finds blank values and fills them in from the raw table |

```bash
mvn test                                   # unit tests, JDK 17+
docker compose up -d --wait                # local Kafka and Schema Registry
GCP_PROJECT=your-project-id ARM=D mvn -B -q compile exec:java \
  -Dexec.mainClass=com.dwh.beam.LoadTest -Dexec.classpathScope=test

GCP_PROJECT=your-project-id ./scripts/dataflow-setup.sh     # once: API, bucket, service account
GCP_PROJECT=your-project-id mvn -B -q compile exec:java -Dexec.mainClass=com.dwh.beam.DataflowLoadTest
GCP_PROJECT=your-project-id ./scripts/dataflow-teardown.sh
```

The Dataflow run costs about 5p, and the job cancels itself on exit and after 30 minutes regardless.
