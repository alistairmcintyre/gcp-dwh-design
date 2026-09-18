package com.dwh.beam;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.joda.time.Duration;

import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * The pipeline under load test. Two sinks, deliberately.
 *
 * <pre>
 *   KafkaIO ──┬──► raw_unparsed    schema_id + payload bytes. Schema never changes, so this can
 *             │                    never be affected by a schema race. Ground truth.
 *             └──► trades_parsed   typed columns. This is where the race happens.
 * </pre>
 *
 * <p>Comparing the two is the test. A row count on the parsed table alone proves nothing: with
 * unknown values ignored, every row lands and the new column is null on all of them.
 *
 * <p>Both sinks use STORAGE_WRITE_API, which is the point -- the write stream caches the table
 * schema per connection, and that cache is the thing whose staleness we are trying to measure.
 */
public final class LoadTestPipeline {

    private LoadTestPipeline() {}

    public static Pipeline build(String project, String rawTable, String parsedTable,
                                 String bootstrap, String topic, String registryUrl,
                                 boolean autoSchemaUpdate, boolean ignoreUnknownValues) {

        // blockOnRun defaults to true on the DirectRunner, so run() on an unbounded Kafka source
        // never returns and nothing after it -- including the producer -- ever starts.
        Pipeline p = Pipeline.create(PipelineOptionsFactory
                .fromArgs("--runner=DirectRunner", "--blockOnRun=false").create());

        PCollection<KV<String, byte[]>> messages = p.apply("ReadKafka",
                KafkaIO.<String, byte[]>read()
                        .withBootstrapServers(bootstrap)
                        .withTopic(topic)
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(ByteArrayDeserializer.class)
                        .withConsumerConfigUpdates(Map.of(
                                "auto.offset.reset", "earliest",
                                "group.id", groupId(topic)))
                        // Without this KafkaIO never commits back to Kafka, and consumer lag
                        // reads as permanently empty rather than tracking what was processed.
                        .commitOffsetsInFinalize()
                        .withoutMetadata());

        // ---- the safety net: no decode, no schema, nothing to go stale ----------------------------
        messages.apply("ToRawRow", ParDo.of(new ToRawRowFn()))
                .apply("WriteRaw", BigQueryIO.writeTableRows()
                        .to(rawTable)
                        .withSchema(rawSchema())
                        .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                        // Mandatory on an unbounded source: it is how often the Storage Write API
                        // commits. It also sets the floor on measured end-to-end latency, so keep it
                        // small relative to the effect being measured.
                        .withTriggeringFrequency(Duration.standardSeconds(5))
                        .withNumStorageWriteApiStreams(1)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        // ---- the path under test ------------------------------------------------------------------
        BigQueryIO.Write<TableRow> parsedWrite = BigQueryIO.writeTableRows()
                .to(parsedTable)
                .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                .withTriggeringFrequency(Duration.standardSeconds(5))
                .withNumStorageWriteApiStreams(1)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                // Lets the write stream notice the table gained a column and reconnect against it.
                // Without this the stream keeps its original schema snapshot for the life of the job.
                ;

        // BEAM FORCES THESE TWO TOGETHER. BigQueryIO validates:
        //   "Auto schema update currently only supported when ignoreUnknownValues also set."
        // with a TODO to lift it. So a Beam pipeline cannot have the write stream refresh itself
        // WITHOUT also silently discarding fields the stream does not yet know about. The choice is
        // between a stream that heals and drops, or one that neither heals nor drops but fails.
        if (autoSchemaUpdate) {
            parsedWrite = parsedWrite.withAutoSchemaUpdate(true).ignoreUnknownValues();
        } else if (ignoreUnknownValues) {
            parsedWrite = parsedWrite.ignoreUnknownValues();
        }

        WriteResult written = messages
                .apply("ToParsedRow", ParDo.of(new ToParsedRowFn(registryUrl)))
                .apply("WriteParsed", parsedWrite);

        // Rows the Storage Write API rejects -- e.g. an unknown field with ignoreUnknownValues off --
        // go to this output. Leave it unconsumed and they vanish with no trace, so the test would
        // report missing rows it could not explain. Record every one with its error.
        written.getFailedStorageApiInserts()
                .apply("ToFailedRow", ParDo.of(new ToFailedRowFn()))
                .apply("WriteFailed", BigQueryIO.writeTableRows()
                        .to(failedTable(parsedTable))
                        .withSchema(failedSchema())
                        .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                        .withTriggeringFrequency(Duration.standardSeconds(5))
                        .withNumStorageWriteApiStreams(1)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        return p;
    }

    public static String groupId(String topic) {
        return "beam-loadtest-" + topic;
    }

    public static String failedTable(String parsedTable) {
        return parsedTable.replace("_parsed", "_failed");
    }

    static TableSchema failedSchema() {
        return new TableSchema().setFields(List.of(
                new TableFieldSchema().setName("trade_id").setType("STRING").setMode("NULLABLE"),
                new TableFieldSchema().setName("schema_id").setType("INTEGER").setMode("NULLABLE"),
                new TableFieldSchema().setName("error_message").setType("STRING").setMode("NULLABLE"),
                new TableFieldSchema().setName("failed_at").setType("TIMESTAMP").setMode("NULLABLE")));
    }

    static class ToFailedRowFn extends DoFn<BigQueryStorageApiInsertError, TableRow> {
        private static final long serialVersionUID = 1L;

        @ProcessElement
        public void process(@Element BigQueryStorageApiInsertError error, OutputReceiver<TableRow> out) {
            TableRow row = error.getRow();
            out.output(new TableRow()
                    .set("trade_id", row == null ? null : row.get("trade_id"))
                    .set("schema_id", row == null ? null : row.get("schema_id"))
                    .set("error_message", error.getErrorMessage())
                    .set("failed_at", java.time.Instant.now().toString()));
        }
    }

    static TableSchema rawSchema() {
        return new TableSchema().setFields(List.of(
                new TableFieldSchema().setName("trade_id").setType("STRING").setMode("NULLABLE"),
                new TableFieldSchema().setName("schema_id").setType("INTEGER").setMode("NULLABLE"),
                new TableFieldSchema().setName("payload_b64").setType("STRING").setMode("NULLABLE"),
                new TableFieldSchema().setName("ingested_at").setType("TIMESTAMP").setMode("NULLABLE")));
    }

    /** No Avro decode at all -- which is exactly why this path cannot lose a new field. */
    static class ToRawRowFn extends DoFn<KV<String, byte[]>, TableRow> {
        private static final long serialVersionUID = 1L;

        @ProcessElement
        public void process(@Element KV<String, byte[]> element, OutputReceiver<TableRow> out) {
            byte[] message = element.getValue();
            out.output(new TableRow()
                    .set("trade_id", element.getKey())
                    .set("schema_id", ConfluentWireFormat.schemaId(message))
                    .set("payload_b64", Base64.getEncoder().encodeToString(message))
                    .set("ingested_at", java.time.Instant.now().toString()));
        }
    }

    static class ToParsedRowFn extends DoFn<KV<String, byte[]>, TableRow> {
        private static final long serialVersionUID = 1L;

        private final String registryUrl;
        private transient GenericRecordDecoder decoder;
        private transient TableRowMapper mapper;

        ToParsedRowFn(String registryUrl) {
            this.registryUrl = registryUrl;
        }

        @Setup
        public void setup() {
            decoder = new GenericRecordDecoder(new HttpSchemaRegistryClient(registryUrl));
            mapper = new TableRowMapper();
        }

        @ProcessElement
        public void process(@Element KV<String, byte[]> element, OutputReceiver<TableRow> out) {
            byte[] message = element.getValue();
            GenericRecord record = decoder.decode(message);
            TableRow row = new TableRow();
            Map<String, Object> mapped = mapper.toRow(record);
            mapped.forEach(row::set);
            // The check column. It exists from day one, so it can never be dropped. A field named
            // here whose own column is null was in the message but did not reach BigQuery.
            row.set("present_fields", mapped.entrySet().stream()
                    .filter(e -> e.getValue() != null).map(Map.Entry::getKey).sorted().toList());
            // Carried so the analysis can separate v1 from v2 rows without re-decoding.
            row.set("schema_id", ConfluentWireFormat.schemaId(message));
            out.output(row);
        }
    }
}
