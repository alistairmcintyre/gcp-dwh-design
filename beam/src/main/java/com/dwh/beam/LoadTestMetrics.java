package com.dwh.beam;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.QueryJobConfiguration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Samples "how far behind is the warehouse" while the load test runs.
 *
 * <p>Beam has no acknowledgement, so there is no unacked count to read. What is measurable is
 * consumer lag (committed offset against end offset, the backlog into the pipeline), produced minus
 * landed (the end-to-end backlog, including anything in flight), and rows landed with the new field.
 * Only the last one catches the failure this test is about: rows can land perfectly well with the
 * new column null on every one of them.
 */
public class LoadTestMetrics {

    public static class Sample {
        public final Instant at;
        public final long secondsIn;
        public final long produced;
        public final long landedTotal;
        public final long landedV2;
        public final long landedV2WithField;
        public final long kafkaLag;
        public final long failed;
        /** From the check column: the message carried the field, the row landed without it. */
        public final long dropped;

        Sample(Instant at, long secondsIn, long produced, long landedTotal,
               long landedV2, long landedV2WithField, long kafkaLag, long failed, long dropped) {
            this.at = at;
            this.secondsIn = secondsIn;
            this.produced = produced;
            this.landedTotal = landedTotal;
            this.landedV2 = landedV2;
            this.landedV2WithField = landedV2WithField;
            this.kafkaLag = kafkaLag;
            this.failed = failed;
            this.dropped = dropped;
        }

        public long backlog() {
            return Math.max(0, produced - landedTotal);
        }

        /** v2 rows that landed without the new field -- silent loss, invisible to a row count. */
        public long missingField() {
            return dropped;
        }
    }

    private final BigQuery bigquery;
    private final String parsedTable;
    private final String bootstrap;
    private final String topic;
    private final List<Sample> samples = new ArrayList<>();
    private final Instant start = Instant.now();
    // Registry ids are global and reused for identical schemas, so v2's id is only known once it is
    // registered. -1 until then, which matches no row.
    private volatile String v2Predicate = "false";

    public void setV2Id(int id) {
        this.v2Predicate = "schema_id = " + id;
    }

    /** For sources whose schema version is not an integer id, e.g. a Pub/Sub revision. */
    public void setV2Predicate(String predicate) {
        this.v2Predicate = predicate;
    }

    public LoadTestMetrics(BigQuery bigquery, String parsedTable, String bootstrap, String topic) {
        this.bigquery = bigquery;
        this.parsedTable = parsedTable;
        this.bootstrap = bootstrap;
        this.topic = topic;
    }

    public Sample sample(long produced) {
        long total = 0;
        long v2 = 0;
        long v2WithField = 0;
        long dropped = 0;
        // Before the DDL runs, fill_price does not exist and a query naming it fails. Fall back to
        // counting rows only, so the backlog is still measured during phase 1.
        try {
            for (var row : query("select count(*) as total, countif(" + v2Predicate + ") as v2, "
                    + "countif(" + v2Predicate + " and fill_price is not null) as v2_with_field, "
                    + "countif('fill_price' in unnest(present_fields) and fill_price is null) as dropped "
                    + "from `" + parsedTable + "`")) {
                total = row.get("total").getLongValue();
                v2 = row.get("v2").getLongValue();
                v2WithField = row.get("v2_with_field").getLongValue();
                dropped = row.get("dropped").getLongValue();
            }
        } catch (Exception noFillPriceYet) {
            try {
                for (var row : query("select count(*) as total, countif(" + v2Predicate + ") as v2 from `"
                        + parsedTable + "`")) {
                    total = row.get("total").getLongValue();
                    v2 = row.get("v2").getLongValue();
                }
            } catch (Exception ignored) {
                // table not queryable yet
            }
        }
        long failed = 0;
        try {
            for (var row : query("select count(*) as n from `"
                    + LoadTestPipeline.failedTable(parsedTable) + "`")) {
                failed = row.get("n").getLongValue();
            }
        } catch (Exception noFailuresTableYet) {
            // created on the first failure
        }
        Sample s = new Sample(Instant.now(), Duration.between(start, Instant.now()).toSeconds(),
                produced, total, v2, v2WithField, kafkaLag(), failed, dropped);
        samples.add(s);
        return s;
    }

    private Iterable<com.google.cloud.bigquery.FieldValueList> query(String sql) throws InterruptedException {
        return bigquery.query(QueryJobConfiguration.of(sql)).iterateAll();
    }

    /** Committed offset versus end offset, summed across partitions. */
    long kafkaLag() {
        if (bootstrap == null) {
            return -1;
        }
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        try (AdminClient admin = AdminClient.create(props)) {
            for (String group : java.util.List.of(LoadTestPipeline.groupId(topic))) {
                ListConsumerGroupOffsetsResult offsets = admin.listConsumerGroupOffsets(group);
                Map<TopicPartition, OffsetAndMetadata> committed =
                        offsets.partitionsToOffsetAndMetadata().get();
                if (committed.isEmpty()) {
                    continue;
                }
                long lag = 0;
                var ends = admin.listOffsets(committed.keySet().stream().collect(
                        java.util.stream.Collectors.toMap(tp -> tp,
                                tp -> org.apache.kafka.clients.admin.OffsetSpec.latest()))).all().get();
                for (var entry : committed.entrySet()) {
                    if (entry.getKey().topic().equals(topic)) {
                        lag += ends.get(entry.getKey()).offset() - entry.getValue().offset();
                    }
                }
                return lag;
            }
        } catch (Exception e) {
            return -1;
        }
        return -1;
    }

    public List<Sample> samples() {
        return samples;
    }

    public void printTimeline() {
        System.out.println();
        System.out.println("  t(s)  produced  landed  backlog  v2_rows  v2_w/field  field_missing  failed  kafka_lag");
        System.out.println("  ----  --------  ------  -------  -------  ----------  -------------  ------  ---------");
        // backlog = produced minus landed in BigQuery: everything published and not yet queryable.
        for (Sample s : samples) {
            System.out.printf("  %4d  %8d  %6d  %7d  %7d  %10d  %13d  %6d  %9s%n",
                    s.secondsIn, s.produced, s.landedTotal, s.backlog(),
                    s.landedV2, s.landedV2WithField, s.missingField(), s.failed,
                    s.kafkaLag < 0 ? "n/a" : String.valueOf(s.kafkaLag));
        }
    }

    /** Seconds from the first v2 row landing to the first v2 row landing WITH the new field. */
    public Long recoverySeconds() {
        Long firstV2 = null;
        for (Sample s : samples) {
            if (firstV2 == null && s.landedV2 > 0) {
                firstV2 = s.secondsIn;
            }
            if (firstV2 != null && s.landedV2WithField > 0) {
                return s.secondsIn - firstV2;
            }
        }
        return null;
    }
}
