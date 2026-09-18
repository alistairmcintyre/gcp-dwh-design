package com.dwh.beam;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Arm B on real Dataflow: Pub/Sub (Avro, schema revisions) into BigQuery through the Storage Write
 * API with {@code withAutoSchemaUpdate}.
 *
 * <p>Two differences from the local run matter. Auto-sharding rather than one fixed stream, since
 * Dataflow picks the number of write streams and changes it as it runs, and streams opened after a
 * schema change are where Beam before 2.63 dropped new fields. And Pub/Sub rather than Kafka, so the
 * schema revision arrives as a message attribute instead of a 5-byte header. Everything after the
 * decode is the same code as the Kafka pipeline.
 */
public final class DataflowAutoUpdatePipeline {

    private DataflowAutoUpdatePipeline() {}

    public static Pipeline build(PipelineOptions options, String subscription, String rawTable, String parsedTable) {
        Pipeline p = Pipeline.create(options);

        PCollection<PubsubMessage> messages = p.apply("ReadPubsub",
                PubsubIO.readMessagesWithAttributes().fromSubscription(subscription));

        // Every message as it arrived, so any dropped value can be decoded again later.
        messages.apply("ToRawRow", ParDo.of(new ToRawRowFn()))
                .apply("WriteRaw", BigQueryIO.writeTableRows()
                        .to(rawTable)
                        .withSchema(new TableSchema().setFields(List.of(
                                field("trade_id", "STRING"),
                                field("schema_revision", "STRING"),
                                field("payload_b64", "STRING"),
                                field("ingested_at", "TIMESTAMP"))))
                        .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                        .withTriggeringFrequency(Duration.standardSeconds(5))
                        .withAutoSharding()
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        WriteResult written = messages
                .apply("DecodeAvro", ParDo.of(new ToParsedRowFn()))
                .apply("WriteParsed", BigQueryIO.writeTableRows()
                        .to(parsedTable)
                        .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                        .withTriggeringFrequency(Duration.standardSeconds(5))
                        .withAutoSharding()
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        // Arm B. Beam refuses the first without the second.
                        .withAutoSchemaUpdate(true)
                        .ignoreUnknownValues());

        written.getFailedStorageApiInserts()
                .apply("ToFailedRow", ParDo.of(new ToFailedRowFn()))
                .apply("WriteFailed", BigQueryIO.writeTableRows()
                        .to(LoadTestPipeline.failedTable(parsedTable))
                        .withSchema(new TableSchema().setFields(List.of(
                                field("trade_id", "STRING"),
                                field("schema_revision", "STRING"),
                                field("error_message", "STRING"),
                                field("failed_at", "TIMESTAMP"))))
                        .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                        .withTriggeringFrequency(Duration.standardSeconds(5))
                        .withAutoSharding()
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        return p;
    }

    private static TableFieldSchema field(String name, String type) {
        return new TableFieldSchema().setName(name).setType(type).setMode("NULLABLE");
    }

    static class ToRawRowFn extends DoFn<PubsubMessage, TableRow> {
        private static final long serialVersionUID = 1L;

        @ProcessElement
        public void process(@Element PubsubMessage message, OutputReceiver<TableRow> out) {
            out.output(new TableRow()
                    .set("trade_id", message.getAttribute("trade_id"))
                    .set("schema_revision", message.getAttribute(PubsubSchemaResolver.REVISION_ID))
                    .set("payload_b64", Base64.getEncoder().encodeToString(message.getPayload()))
                    .set("ingested_at", java.time.Instant.now().toString()));
        }
    }

    static class ToParsedRowFn extends DoFn<PubsubMessage, TableRow> {
        private static final long serialVersionUID = 1L;

        private final PubsubSchemaResolver resolver = new PubsubSchemaResolver();
        private final TableRowMapper mapper = new TableRowMapper();

        @ProcessElement
        public void process(@Element PubsubMessage message, OutputReceiver<TableRow> out) throws Exception {
            String revision = message.getAttribute(PubsubSchemaResolver.REVISION_ID);
            var writerSchema = resolver.resolve(message.getAttribute(PubsubSchemaResolver.SCHEMA_NAME), revision);
            GenericRecord record = new GenericDatumReader<GenericRecord>(writerSchema)
                    .read(null, DecoderFactory.get().binaryDecoder(message.getPayload(), null));

            TableRow row = new TableRow();
            Map<String, Object> mapped = mapper.toRow(record);
            mapped.forEach(row::set);
            // The check column: fields the message carried. Exists from day one, so never dropped.
            row.set("present_fields", mapped.keySet().stream().sorted().toList());
            row.set("schema_revision", revision);
            out.output(row);
        }

        @Teardown
        public void teardown() {
            resolver.close();
        }
    }

    static class ToFailedRowFn extends DoFn<BigQueryStorageApiInsertError, TableRow> {
        private static final long serialVersionUID = 1L;

        @ProcessElement
        public void process(@Element BigQueryStorageApiInsertError error, OutputReceiver<TableRow> out) {
            TableRow row = error.getRow();
            out.output(new TableRow()
                    .set("trade_id", row == null ? null : row.get("trade_id"))
                    .set("schema_revision", row == null ? null : row.get("schema_revision"))
                    .set("error_message", error.getErrorMessage())
                    .set("failed_at", java.time.Instant.now().toString()));
        }
    }
}
