package com.dwh.beam;

import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SchemaServiceClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.monitoring.v3.ListTimeSeriesRequest;
import com.google.monitoring.v3.TimeInterval;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import com.google.pubsub.v1.Encoding;
import com.google.pubsub.v1.ProjectName;
import com.google.pubsub.v1.SchemaName;
import com.google.pubsub.v1.SchemaSettings;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Arm B on a deployed Dataflow job.
 *
 * <pre>
 *   warm-up  publish v1 until rows land       (Dataflow takes a few minutes to start workers)
 *   phase 1  V1_SECONDS of v1
 *   switch   commit v2 revision + add the BigQuery column, at the same moment
 *   phase 2  V2_SECONDS of v2
 *   drain    DRAIN_SECONDS, then cancel the job
 * </pre>
 *
 * <p>The job is cancelled on the way out, including on Ctrl+C, and Dataflow itself stops it after
 * MAX_RUNTIME_SECONDS in case this process dies. Needs scripts/dataflow-setup.sh run once first.
 */
public final class DataflowLoadTest {

    private static final String PROJECT = System.getenv("GCP_PROJECT");
    private static final String REGION = env("REGION", "europe-west2");
    private static final String DATASET = env("BQ_DATASET", "scratch");
    private static final String BUCKET = env("BUCKET", PROJECT + "-dataflow");
    private static final String SERVICE_ACCOUNT =
            env("SERVICE_ACCOUNT", "dataflow-loadtest@" + PROJECT + ".iam.gserviceaccount.com");
    private static final int RATE_PER_SEC = Integer.parseInt(env("RATE_PER_SEC", "100"));
    private static final int V1_SECONDS = Integer.parseInt(env("V1_SECONDS", "60"));
    private static final int V2_SECONDS = Integer.parseInt(env("V2_SECONDS", "300"));
    private static final int DRAIN_SECONDS = Integer.parseInt(env("DRAIN_SECONDS", "90"));
    private static final int MAX_RUNTIME_SECONDS = Integer.parseInt(env("MAX_RUNTIME_SECONDS", "1800"));

    private static final String SCHEMA_ID = "loadtest-trade";
    private static final String TOPIC = "loadtest-dataflow";
    private static final String SUBSCRIPTION = "loadtest-dataflow-sub";
    private static final String RAW = "loadtest_dataflow_raw";
    private static final String PARSED = "loadtest_dataflow_parsed";

    private static final AtomicLong PUBLISHED = new AtomicLong();
    private static final AtomicLong PUBLISH_ERRORS = new AtomicLong();

    private DataflowLoadTest() {}

    public static void main(String[] args) throws Exception {
        if (PROJECT == null) {
            throw new IllegalStateException("Set GCP_PROJECT");
        }
        BigQuery bq = BigQueryOptions.newBuilder().setProjectId(PROJECT).build().getService();
        String schemaName = SchemaName.of(PROJECT, SCHEMA_ID).toString();

        System.out.printf("%n=== ARM B on Dataflow (Beam %s) ===%n",
                org.apache.beam.sdk.util.ReleaseInfo.getReleaseInfo().getVersion());
        resetPubsub(schemaName);
        resetTables(bq);

        DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(
                "--runner=DataflowRunner",
                "--project=" + PROJECT,
                "--region=" + REGION,
                "--jobName=schema-autoupdate-b-" + System.currentTimeMillis() / 1000,
                "--tempLocation=gs://" + BUCKET + "/temp",
                "--stagingLocation=gs://" + BUCKET + "/staging",
                "--serviceAccount=" + SERVICE_ACCOUNT,
                "--workerMachineType=e2-standard-2",
                "--numWorkers=1",
                "--maxNumWorkers=2",
                "--diskSizeGb=30",
                "--enableStreamingEngine",
                "--streaming",
                // Dataflow cancels the job itself after this, even if this process is killed.
                "--dataflowServiceOptions=max_workflow_runtime_walltime_seconds=" + MAX_RUNTIME_SECONDS)
                .as(DataflowPipelineOptions.class);

        DataflowPipelineJob job = (DataflowPipelineJob) DataflowAutoUpdatePipeline.build(options,
                SubscriptionName.of(PROJECT, SUBSCRIPTION).toString(),
                PROJECT + ":" + DATASET + "." + RAW,
                PROJECT + ":" + DATASET + "." + PARSED).run();
        Thread cancelOnExit = new Thread(() -> cancel(job));
        Runtime.getRuntime().addShutdownHook(cancelOnExit);
        System.out.printf("job %s launched%n  https://console.cloud.google.com/dataflow/jobs/%s/%s?project=%s%n",
                job.getJobId(), REGION, job.getJobId(), PROJECT);

        Instant testStart = Instant.now();
        LoadTestMetrics metrics = new LoadTestMetrics(bq, PROJECT + "." + DATASET + "." + PARSED, null, TOPIC);
        String v2Revision = null;
        try (SchemaServiceClient schemas = SchemaServiceClient.create()) {
            Publisher publisher = Publisher.newBuilder(TopicName.of(PROJECT, TOPIC)).build();
            try {
                org.apache.avro.Schema v1 = new org.apache.avro.Schema.Parser().parse(SchemaRegistryOps.V1);
                org.apache.avro.Schema v2 = new org.apache.avro.Schema.Parser().parse(SchemaRegistryOps.V2);

                System.out.println("\n-- warm-up: v1 until the first rows land (worker start takes minutes) --");
                if (!publishFor(publisher, v1, 900, metrics, true)) {
                    throw new IllegalStateException("no rows landed within 15 minutes; check the job in the console");
                }
                mark(metrics, "first rows landed");

                System.out.printf("%n-- phase 1: %ds of v1 --%n", V1_SECONDS);
                publishFor(publisher, v1, V1_SECONDS, metrics, false);

                // The race, as in arm B: new revision and new column at the same moment.
                v2Revision = schemas.commitSchema(schemaName, com.google.pubsub.v1.Schema.newBuilder()
                        .setName(schemaName).setType(com.google.pubsub.v1.Schema.Type.AVRO)
                        .setDefinition(SchemaRegistryOps.V2).build()).getRevisionId();
                var added = new BigQueryDdlReconciler(bq).reconcile(TableId.of(PROJECT, DATASET, PARSED), v2);
                metrics.setV2Predicate("schema_revision = '" + v2Revision + "'");
                mark(metrics, "v2 revision " + v2Revision + " committed, DDL added " + added);

                System.out.printf("%n-- phase 2: %ds of v2 --%n", V2_SECONDS);
                publishFor(publisher, v2, V2_SECONDS, metrics, false);
                mark(metrics, "v2 publishing stopped");

                System.out.printf("%n-- draining for %ds --%n", DRAIN_SECONDS);
                long drainUntil = System.currentTimeMillis() + DRAIN_SECONDS * 1000L;
                while (System.currentTimeMillis() < drainUntil) {
                    Thread.sleep(10_000);
                    print(metrics.sample(PUBLISHED.get()));
                }
            } finally {
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            }
        } finally {
            cancel(job);
            Runtime.getRuntime().removeShutdownHook(cancelOnExit);
        }

        metrics.printTimeline();
        System.out.printf("%npublished=%d  publish_errors=%d  recovery=%s%n", PUBLISHED.get(), PUBLISH_ERRORS.get(),
                metrics.recoverySeconds() == null ? "new field never landed" : metrics.recoverySeconds() + "s");
        if (v2Revision != null) {
            summarise(bq, v2Revision);
        }
        printPubsubBacklog(testStart);
        System.exit(0);
    }

    // ---- Pub/Sub ------------------------------------------------------------------------------------

    /** Subscription, topic and schema from scratch, so the run starts with one revision. */
    private static void resetPubsub(String schemaName) throws Exception {
        try (SubscriptionAdminClient subs = SubscriptionAdminClient.create();
             TopicAdminClient topics = TopicAdminClient.create();
             SchemaServiceClient schemas = SchemaServiceClient.create()) {
            ignoreNotFound(() -> subs.deleteSubscription(SubscriptionName.of(PROJECT, SUBSCRIPTION)));
            ignoreNotFound(() -> topics.deleteTopic(TopicName.of(PROJECT, TOPIC)));
            ignoreNotFound(() -> schemas.deleteSchema(schemaName));

            schemas.createSchema(ProjectName.of(PROJECT), com.google.pubsub.v1.Schema.newBuilder()
                    .setType(com.google.pubsub.v1.Schema.Type.AVRO)
                    .setDefinition(SchemaRegistryOps.V1).build(), SCHEMA_ID);
            topics.createTopic(Topic.newBuilder()
                    .setName(TopicName.of(PROJECT, TOPIC).toString())
                    .setSchemaSettings(SchemaSettings.newBuilder()
                            .setSchema(schemaName).setEncoding(Encoding.BINARY)).build());
            subs.createSubscription(Subscription.newBuilder()
                    .setName(SubscriptionName.of(PROJECT, SUBSCRIPTION).toString())
                    .setTopic(TopicName.of(PROJECT, TOPIC).toString())
                    .setAckDeadlineSeconds(60).build());
            System.out.printf("reset Pub/Sub: schema %s (v1), topic %s, subscription %s%n", SCHEMA_ID, TOPIC, SUBSCRIPTION);
        }
    }

    /** Publishes at a fixed rate, sampling every ten seconds. With untilLanded, stops at the first landed row. */
    private static boolean publishFor(Publisher publisher, org.apache.avro.Schema schema, int seconds,
                                      LoadTestMetrics metrics, boolean untilLanded) throws Exception {
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        long intervalNanos = 1_000_000_000L / RATE_PER_SEC;
        long deadline = System.nanoTime() + seconds * 1_000_000_000L;
        long nextSample = System.currentTimeMillis() + 10_000;
        long next = System.nanoTime();
        while (System.nanoTime() < deadline) {
            long n = PUBLISHED.incrementAndGet();
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bytes, null);
            writer.write(LoadTest.trade(schema, n), encoder);
            encoder.flush();
            ApiFutures.addCallback(publisher.publish(com.google.pubsub.v1.PubsubMessage.newBuilder()
                    .setData(ByteString.copyFrom(bytes.toByteArray()))
                    .putAttributes("trade_id", "T" + n).build()), new ApiFutureCallback<>() {
                        @Override public void onFailure(Throwable t) {
                            if (PUBLISH_ERRORS.getAndIncrement() == 0) {
                                System.out.println("  publish failed: " + t.getMessage());
                            }
                        }
                        @Override public void onSuccess(String id) {}
                    }, MoreExecutors.directExecutor());

            if (System.currentTimeMillis() >= nextSample) {
                var s = metrics.sample(PUBLISHED.get());
                print(s);
                nextSample += 10_000;
                if (untilLanded && s.landedTotal > 0) {
                    return true;
                }
            }
            next += intervalNanos;
            long sleep = next - System.nanoTime();
            if (sleep > 0) {
                Thread.sleep(sleep / 1_000_000L, (int) (sleep % 1_000_000L));
            }
        }
        return !untilLanded;
    }

    /** num_undelivered_messages from Cloud Monitoring. Sampled once a minute and published late. */
    private static void printPubsubBacklog(Instant from) throws Exception {
        System.out.println("\n-- Pub/Sub backlog (Cloud Monitoring, waiting 120s for the last points) --");
        Thread.sleep(120_000);
        try (MetricServiceClient client = MetricServiceClient.create()) {
            List<String> lines = new ArrayList<>();
            for (String metric : List.of("num_undelivered_messages", "oldest_unacked_message_age")) {
                var request = ListTimeSeriesRequest.newBuilder()
                        .setName(com.google.monitoring.v3.ProjectName.of(PROJECT).toString())
                        .setFilter("metric.type=\"pubsub.googleapis.com/subscription/" + metric
                                + "\" AND resource.labels.subscription_id=\"" + SUBSCRIPTION + "\"")
                        .setInterval(TimeInterval.newBuilder()
                                .setStartTime(Timestamps.fromMillis(from.toEpochMilli()))
                                .setEndTime(Timestamps.fromMillis(System.currentTimeMillis())))
                        .setView(ListTimeSeriesRequest.TimeSeriesView.FULL).build();
                for (var series : client.listTimeSeries(request).iterateAll()) {
                    for (var point : series.getPointsList()) {
                        long at = Timestamps.toMillis(point.getInterval().getEndTime());
                        lines.add(String.format("  +%4ds  %-27s %d", (at - from.toEpochMilli()) / 1000,
                                metric, point.getValue().getInt64Value()));
                    }
                }
            }
            lines.sort(null);
            lines.forEach(System.out::println);
            if (lines.isEmpty()) {
                System.out.println("  (no points yet -- see the subscription's Metrics tab in the console)");
            }
        }
    }

    // ---- BigQuery -----------------------------------------------------------------------------------

    private static void resetTables(BigQuery bq) {
        bq.delete(TableId.of(PROJECT, DATASET, RAW));
        bq.delete(TableId.of(PROJECT, DATASET, LoadTestPipeline.failedTable(PARSED)));
        TableId parsed = TableId.of(PROJECT, DATASET, PARSED);
        bq.delete(parsed);
        bq.create(TableInfo.of(parsed, StandardTableDefinition.of(com.google.cloud.bigquery.Schema.of(
                nullable("trade_id", LegacySQLTypeName.STRING),
                nullable("client_id", LegacySQLTypeName.STRING),
                nullable("instrument_id", LegacySQLTypeName.STRING),
                nullable("quantity", LegacySQLTypeName.INTEGER),
                nullable("executed_at", LegacySQLTypeName.TIMESTAMP),
                nullable("produced_at", LegacySQLTypeName.TIMESTAMP),
                nullable("schema_revision", LegacySQLTypeName.STRING),
                Field.newBuilder("present_fields", LegacySQLTypeName.STRING).setMode(Field.Mode.REPEATED).build()))));
        System.out.printf("reset tables: %s.%s (v1 shape, no fill_price), %s%n", DATASET, PARSED, RAW);
    }

    /** Where the gap was, in the producer's own clock. */
    private static void summarise(BigQuery bq, String v2Revision) throws Exception {
        String t = "`" + PROJECT + "." + DATASET + ".";
        String sql = "select "
                + "(select count(*) from " + t + RAW + "`) raw_rows, "
                + "count(*) parsed_rows, count(distinct trade_id) distinct_trades, "
                + "countif(schema_revision = '" + v2Revision + "') v2_rows, "
                + "countif(schema_revision = '" + v2Revision + "' and fill_price is not null) v2_with_field, "
                + "countif('fill_price' in unnest(present_fields) and fill_price is null) dropped, "
                + "min(if(schema_revision = '" + v2Revision + "', produced_at, null)) first_v2_sent, "
                + "min(if('fill_price' in unnest(present_fields) and fill_price is null, produced_at, null)) first_dropped_sent, "
                + "max(if('fill_price' in unnest(present_fields) and fill_price is null, produced_at, null)) last_dropped_sent "
                + "from " + t + PARSED + "`";
        System.out.println();
        for (var row : bq.query(QueryJobConfiguration.of(sql)).iterateAll()) {
            for (var f : List.of("raw_rows", "parsed_rows", "distinct_trades", "v2_rows", "v2_with_field", "dropped",
                    "first_v2_sent", "first_dropped_sent", "last_dropped_sent")) {
                var v = row.get(f);
                System.out.printf("  %-19s %s%n", f, v.isNull() ? "-" : f.endsWith("_sent")
                        ? Instant.ofEpochMilli(v.getTimestampValue() / 1000) : v.getStringValue());
            }
        }
    }

    // ---- helpers ------------------------------------------------------------------------------------

    private static volatile boolean cancelled;

    private static synchronized void cancel(DataflowPipelineJob job) {
        if (cancelled) {
            return;
        }
        cancelled = true;
        try {
            System.out.println("\ncancelling Dataflow job " + job.getJobId());
            job.cancel();
            PipelineResult.State state = job.waitUntilFinish(org.joda.time.Duration.standardMinutes(5));
            System.out.println("job state: " + state);
        } catch (Exception e) {
            System.out.println("CANCEL FAILED, stop it in the console: " + e.getMessage());
        }
    }

    private static void mark(LoadTestMetrics metrics, String event) {
        var samples = metrics.samples();
        long t = samples.isEmpty() ? 0 : samples.get(samples.size() - 1).secondsIn;
        System.out.printf("  >> after t=%d: %s%n", t, event);
    }

    private static void print(LoadTestMetrics.Sample s) {
        System.out.printf("  t=%-4d published=%-6d landed=%-6d backlog=%-6d v2=%-6d w/field=%-6d dropped=%-5d failed=%d%n",
                s.secondsIn, s.produced, s.landedTotal, s.backlog(), s.landedV2, s.landedV2WithField,
                s.missingField(), s.failed);
    }

    private static Field nullable(String name, LegacySQLTypeName type) {
        return Field.newBuilder(name, type).setMode(Field.Mode.NULLABLE).build();
    }

    private static void ignoreNotFound(Runnable r) {
        try {
            r.run();
        } catch (NotFoundException ignored) {
            // already gone
        }
    }

    private static String env(String name, String fallback) {
        return System.getenv().getOrDefault(name, fallback);
    }
}
