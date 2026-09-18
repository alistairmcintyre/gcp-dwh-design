package com.dwh.beam;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.PipelineResult;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Load test of schema evolution under a live producer. Pick the arm with ARM=A|B|C|D.
 *
 * <pre>
 *   A   column added 60s before v2 traffic, no auto-update
 *   B   auto-update on, column added as v2 traffic starts
 *   C   neither: the append is expected to fail
 *   D   column added 60s ahead with v1 still flowing, plus auto-update
 * </pre>
 *
 * <p>Each arm runs a minute of v1 at 100/sec, then five minutes of v2, sampling every ten seconds.
 * Five minutes because the docs put schema detection at minutes: a one-minute window can end before
 * the stream recovers, which would read as loss when it was only a short window.
 *
 * <pre>
 *   GCP_PROJECT=your-project-id ARM=B mvn -q exec:java \
 *     -Dexec.mainClass=com.dwh.beam.LoadTest -Dexec.classpathScope=test
 * </pre>
 */
public final class LoadTest {

    private static final String PROJECT = System.getenv("GCP_PROJECT");
    private static final String DATASET = System.getenv().getOrDefault("BQ_DATASET", "scratch");
    private static final String ARM = System.getenv().getOrDefault("ARM", "A").toUpperCase();
    private static final String BOOTSTRAP =
            System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String REGISTRY =
            System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081");

    private static final int RATE_PER_SEC = Integer.parseInt(
            System.getenv().getOrDefault("RATE_PER_SEC", "100"));
    private static final int V1_SECONDS = Integer.parseInt(
            System.getenv().getOrDefault("V1_SECONDS", "60"));
    private static final int V2_SECONDS = Integer.parseInt(
            System.getenv().getOrDefault("V2_SECONDS", "300"));

    private static final int GAP_SECONDS = Integer.parseInt(
            System.getenv().getOrDefault("GAP_SECONDS", "60"));
    private static final int DRAIN_SECONDS = Integer.parseInt(
            System.getenv().getOrDefault("DRAIN_SECONDS", "90"));

    private static final AtomicLong PRODUCED = new AtomicLong();

    private LoadTest() {}

    public static void main(String[] args) throws Exception {
        if (PROJECT == null) {
            throw new IllegalStateException("Set GCP_PROJECT");
        }
        String topic = "loadtest-" + ARM.toLowerCase();
        String rawTable = PROJECT + ":" + DATASET + ".loadtest_" + ARM.toLowerCase() + "_raw";
        String parsedTable = PROJECT + ":" + DATASET + ".loadtest_" + ARM.toLowerCase() + "_parsed";

        BigQuery bq = BigQueryOptions.newBuilder().setProjectId(PROJECT).build().getService();

        System.out.printf("%n=== ARM %s ===%n", ARM);
        System.out.printf("topic=%s  rate=%d/s  v1=%ds  v2=%ds%n", topic, RATE_PER_SEC, V1_SECONDS, V2_SECONDS);

        createTopic(topic);
        // Only this test's own scratch tables. The raw and failed sinks recreate themselves on first write.
        for (String t : java.util.List.of(rawTable, LoadTestPipeline.failedTable(parsedTable))) {
            bq.delete(TableId.of(PROJECT, DATASET, t.substring(t.lastIndexOf('.') + 1)));
        }
        recreateParsedTable(bq, parsedTable);
        SchemaRegistryOps.deleteSubject(REGISTRY, topic + "-value");
        registerSchema(topic, "v1");
        int v1Id = SchemaRegistryOps.latestId(REGISTRY, topic);
        System.out.println("registered v1 as schema id " + v1Id);

        // ARM A  DDL runs ahead, so no unknown field is ever presented. Neither flag needed.
        // ARM B  autoSchemaUpdate -- which Beam forces to carry ignoreUnknownValues with it.
        // ARM C  neither flag: the append fails on an unknown field rather than dropping it.
        // ARM D  DDL ahead (like A) AND autoSchemaUpdate (like B). v1 keeps flowing during the gap,
        //        as it would between registering at 08:00 and v2 arriving at 09:00. That matters:
        //        Beam only learns about a new column from BigQuery's reply to an append.
        boolean autoUpdate = ARM.equals("B") || ARM.equals("D");
        boolean ignoreUnknown = false;
        var pipeline = LoadTestPipeline.build(PROJECT, rawTable, parsedTable, BOOTSTRAP, topic,
                REGISTRY, autoUpdate, ignoreUnknown);
        PipelineResult result = pipeline.run();
        System.out.printf("pipeline started (autoSchemaUpdate=%s, ignoreUnknownValues=%s)%n",
                autoUpdate, autoUpdate || ignoreUnknown);

        LoadTestMetrics metrics = new LoadTestMetrics(bq, parsedTable.replace(':', '.'), BOOTSTRAP, topic);

        // ---- phase 1: v1 traffic -------------------------------------------------------------------
        System.out.printf("%n-- phase 1: %ds of v1 --%n", V1_SECONDS);
        produceFor(topic, v1Id, V1_SECONDS, metrics);

        // ---- register v2, and run the DDL according to the arm ------------------------------------
        registerSchema(topic, "v2");
        int v2Id = SchemaRegistryOps.latestId(REGISTRY, topic);
        System.out.println("registered v2 as schema id " + v2Id);
        metrics.setV2Id(v2Id);

        if (ARM.equals("A")) {
            // The intended design: the column exists before a single v2 message is produced.
            applyDdl(bq, parsedTable, topic);
            System.out.println("ARM A: DDL applied, waiting 60s before any v2 traffic");
            sleepSampling(60, metrics);
        } else if (ARM.equals("D")) {
            applyDdl(bq, parsedTable, topic);
            System.out.printf("ARM D: DDL applied, v1 traffic continues for %ds before any v2%n", GAP_SECONDS);
            produceFor(topic, v1Id, GAP_SECONDS, metrics);
        } else {
            // Arms B and C: the DDL and the traffic start together. This is the race.
            System.out.println("ARM " + ARM + ": DDL and v2 traffic start together");
            applyDdl(bq, parsedTable, topic);
        }

        // ---- phase 2: v2 traffic -------------------------------------------------------------------
        System.out.printf("%n-- phase 2: %ds of v2 --%n", V2_SECONDS);
        produceFor(topic, v2Id, V2_SECONDS, metrics);

        System.out.printf("%n-- draining for %ds --%n", DRAIN_SECONDS);
        sleepSampling(DRAIN_SECONDS, metrics);

        try {
            result.cancel();
        } catch (Exception e) {
            System.out.println("(pipeline cancel: " + e.getMessage() + ")");
        }

        metrics.printTimeline();
        Long recovery = metrics.recoverySeconds();
        System.out.printf("%nproduced=%d  recovery=%s%n", PRODUCED.get(),
                recovery == null ? "new field never landed" : recovery + "s");
        System.out.printf("Query: select schema_id, count(*) rows, countif(fill_price is not null) with_field "
                + "from `%s` group by 1 order by 1%n", parsedTable.replace(':', '.'));
        // The DirectRunner leaves non-daemon threads behind; without this exec:java never returns.
        System.exit(0);
    }

    /**
     * KafkaIO needs the topic to exist when the pipeline starts, and auto-create only fires on first
     * produce -- which happens after. Three partitions so consumer lag is measured across more than
     * one, which is where ordering assumptions usually break.
     */
    private static void createTopic(String topic) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP);
        try (AdminClient admin = AdminClient.create(props)) {
            if (admin.listTopics().names().get().contains(topic)) {
                admin.deleteTopics(java.util.List.of(topic)).all().get();
                Thread.sleep(3000);
            }
            admin.createTopics(java.util.List.of(new NewTopic(topic, 3, (short) 1))).all().get();
            Thread.sleep(2000);
            System.out.println("created topic " + topic + " (3 partitions)");
        }
    }

    /** Fresh v1-shaped table each run, so an arm never inherits the previous arm's column. */
    private static void recreateParsedTable(BigQuery bq, String table) {
        TableId id = TableId.of(PROJECT, DATASET, table.substring(table.lastIndexOf('.') + 1));
        bq.delete(id);
        bq.create(TableInfo.of(id, StandardTableDefinition.of(Schema.of(
                Field.newBuilder("trade_id", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("client_id", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("instrument_id", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("quantity", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("executed_at", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("produced_at", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("schema_id", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("present_fields", LegacySQLTypeName.STRING).setMode(Field.Mode.REPEATED).build()))));
        System.out.println("recreated " + table + " with the v1 shape (no fill_price)");
    }

    private static void applyDdl(BigQuery bq, String parsedTable, String topic) throws Exception {
        TableId id = TableId.of(PROJECT, DATASET, parsedTable.substring(parsedTable.lastIndexOf('.') + 1));
        var added = new BigQueryDdlReconciler(bq)
                .reconcile(id, SchemaRegistryOps.latestSchema(REGISTRY, topic));
        System.out.println("DDL step added: " + (added.isEmpty() ? "(nothing)" : added));
    }

    private static void registerSchema(String topic, String version) throws Exception {
        SchemaRegistryOps.register(REGISTRY, topic + "-value", version);
        System.out.println("registered " + version + " for " + topic);
    }

    /** Trade n. fill_price is derived from n, so any landed value can be checked exactly. */
    static GenericRecord trade(org.apache.avro.Schema schema, long n) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("trade_id", "T" + n);
        record.put("client_id", "cli-" + (n % 500));
        record.put("instrument_id", "EURUSD");
        record.put("quantity", 10L + (n % 50));
        record.put("executed_at", Instant.now().toEpochMilli() * 1000L);
        record.put("produced_at", Instant.now().toEpochMilli() * 1000L);
        if (schema.getField("fill_price") != null) {
            BigDecimal price = new BigDecimal("1.0" + (80000 + (n % 20000))).setScale(6);
            record.put("fill_price", ByteBuffer.wrap(price.unscaledValue().toByteArray()));
        }
        return record;
    }

    /** Produces at a fixed rate, sampling every ten seconds. */
    private static void produceFor(String topic, int schemaId, int seconds, LoadTestMetrics metrics)
            throws Exception {
        org.apache.avro.Schema schema = new HttpSchemaRegistryClient(REGISTRY).byId(schemaId);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        long intervalNanos = 1_000_000_000L / RATE_PER_SEC;
        long deadline = System.nanoTime() + seconds * 1_000_000_000L;
        long nextSample = System.currentTimeMillis() + 10_000;

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            long next = System.nanoTime();
            while (System.nanoTime() < deadline) {
                long n = PRODUCED.incrementAndGet();
                String tradeId = "T" + n;
                GenericRecord record = trade(schema, n);
                producer.send(new ProducerRecord<>(topic, tradeId,
                        ConfluentWireFormat.frame(schemaId, record)));

                if (System.currentTimeMillis() >= nextSample) {
                    var s = metrics.sample(PRODUCED.get());
                    System.out.printf("  t=%-4d produced=%-6d landed=%-6d backlog=%-6d v2=%-6d w/field=%-6d failed=%-6d lag=%s%n",
                            s.secondsIn, s.produced, s.landedTotal, s.backlog(),
                            s.landedV2, s.landedV2WithField, s.failed, s.kafkaLag < 0 ? "n/a" : s.kafkaLag);
                    nextSample += 10_000;
                }

                next += intervalNanos;
                long sleep = next - System.nanoTime();
                if (sleep > 0) {
                    Thread.sleep(sleep / 1_000_000L, (int) (sleep % 1_000_000L));
                }
            }
        }
    }

    private static void sleepSampling(int seconds, LoadTestMetrics metrics) throws Exception {
        for (int i = 0; i < seconds; i += 10) {
            Thread.sleep(10_000);
            var s = metrics.sample(PRODUCED.get());
            System.out.printf("  t=%-4d produced=%-6d landed=%-6d backlog=%-6d v2=%-6d w/field=%-6d failed=%-6d lag=%s%n",
                    s.secondsIn, s.produced, s.landedTotal, s.backlog(),
                    s.landedV2, s.landedV2WithField, s.failed, s.kafkaLag < 0 ? "n/a" : s.kafkaLag);
        }
    }
}
