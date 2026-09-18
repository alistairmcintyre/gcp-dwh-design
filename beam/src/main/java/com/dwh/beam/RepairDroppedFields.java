package com.dwh.beam;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.JobInfo.CreateDisposition;
import com.google.cloud.bigquery.JobInfo.WriteDisposition;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * The retry for auto-update's gap. With {@code withAutoSchemaUpdate}, Beam must also ignore unknown
 * values, so for the first seconds after a new column appears, rows land with that column null and
 * no error. Nothing in the pipeline can see this happen. This job finds and fixes it afterwards.
 *
 * <ol>
 *   <li><b>Find.</b> Rows with a null column. If the table has the {@code present_fields} check
 *       column, only rows whose message named that field count -- a cheap, exact test. Without it,
 *       every null is a suspect and step 2 decides.
 *   <li><b>Decode again.</b> Join to the raw table on trade_id and decode the original message with
 *       the registry, exactly as the pipeline did.
 *   <li><b>Write back.</b> Load the decoded values into a temporary table, then MERGE, filling only
 *       columns that are null in the table and not null in the message. Nothing else is touched, so
 *       running it twice is safe.
 * </ol>
 *
 * <p>DML is allowed on rows the Storage Write API (gRPC) wrote even in the last 30 minutes, unlike
 * the legacy streaming buffer, so this can run straight after the gap.
 */
public final class RepairDroppedFields {

    private static final String PROJECT = System.getenv("GCP_PROJECT");
    private static final String DATASET = System.getenv().getOrDefault("BQ_DATASET", "scratch");
    private static final String PARSED = System.getenv("PARSED_TABLE");
    private static final String RAW = System.getenv("RAW_TABLE");
    private static final String REGISTRY =
            System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081");

    /** Pipeline bookkeeping, not message fields. */
    private static final Set<String> NOT_DATA = Set.of("trade_id", "schema_id", "present_fields");

    private RepairDroppedFields() {}

    public static void main(String[] args) throws Exception {
        if (PROJECT == null || PARSED == null || RAW == null) {
            throw new IllegalStateException("Set GCP_PROJECT, PARSED_TABLE and RAW_TABLE");
        }
        BigQuery bq = BigQueryOptions.newBuilder().setProjectId(PROJECT).build().getService();
        String location = bq.getDataset(DATASET).getLocation();
        String parsed = "`" + PROJECT + "." + DATASET + "." + PARSED + "`";
        String raw = "`" + PROJECT + "." + DATASET + "." + RAW + "`";

        Schema schema = bq.getTable(TableId.of(PROJECT, DATASET, PARSED)).getDefinition().getSchema();
        boolean hasCheckColumn = schema.getFields().stream().anyMatch(f -> f.getName().equals("present_fields"));
        List<Field> dataFields = schema.getFields().stream()
                .filter(f -> !NOT_DATA.contains(f.getName()))
                .filter(f -> f.getMode() != Field.Mode.REPEATED)
                .toList();

        // ---- 1. find ------------------------------------------------------------------------------
        String suspect = dataFields.stream()
                .map(f -> hasCheckColumn
                        ? "('" + f.getName() + "' in unnest(p.present_fields) and p.`" + f.getName() + "` is null)"
                        : "p.`" + f.getName() + "` is null")
                .collect(Collectors.joining(" or "));
        String nullColumns = "array_concat(" + dataFields.stream()
                .map(f -> "if(p.`" + f.getName() + "` is null, ['" + f.getName() + "'], cast([] as array<string>))")
                .collect(Collectors.joining(", ")) + ")";
        String findSql = "select p.trade_id, r.payload_b64, " + nullColumns + " as null_columns "
                + "from " + parsed + " p join " + raw + " r using (trade_id) where " + suspect;
        System.out.printf("check column present_fields: %s%n", hasCheckColumn ? "yes (exact)" : "no (every null is a suspect)");

        // ---- 2. decode again ----------------------------------------------------------------------
        GenericRecordDecoder decoder = new GenericRecordDecoder(new HttpSchemaRegistryClient(REGISTRY));
        TableRowMapper mapper = new TableRowMapper();
        List<Map<String, Object>> decoded = new ArrayList<>();
        Set<String> repairable = new LinkedHashSet<>();
        long suspects = 0;
        for (var row : bq.query(QueryJobConfiguration.of(findSql)).iterateAll()) {
            suspects++;
            Map<String, Object> values = mapper.toRow(decoder.decode(
                    Base64.getDecoder().decode(row.get("payload_b64").getStringValue())));
            boolean fixesSomething = false;
            for (FieldValue c : row.get("null_columns").getRepeatedValue()) {
                if (values.get(c.getStringValue()) != null) {
                    repairable.add(c.getStringValue());
                    fixesSomething = true;
                }
            }
            if (fixesSomething) {
                decoded.add(values);
            }
        }
        System.out.printf("suspect rows: %d, rows whose message has a value the row lacks: %d, columns: %s%n",
                suspects, decoded.size(), repairable);
        if (decoded.isEmpty()) {
            System.out.println("nothing to repair");
            return;
        }

        // ---- 3. write back ------------------------------------------------------------------------
        List<Field> tmpFields = new ArrayList<>();
        tmpFields.add(Field.of("trade_id", schema.getFields().get("trade_id").getType()));
        dataFields.stream().filter(f -> repairable.contains(f.getName()))
                .forEach(f -> tmpFields.add(f.toBuilder().setMode(Field.Mode.NULLABLE).build()));

        StringBuilder json = new StringBuilder();
        for (Map<String, Object> values : decoded) {
            json.append('{').append(tmpFields.stream()
                    .filter(f -> values.get(f.getName()) != null)
                    .map(f -> SchemaRegistryOps.jsonString(f.getName()) + ":" + toJson(values.get(f.getName())))
                    .collect(Collectors.joining(","))).append("}\n");
        }

        String tmpName = PARSED + "_repair_" + UUID.randomUUID().toString().substring(0, 8);
        TableId tmpId = TableId.of(PROJECT, DATASET, tmpName);
        try {
            WriteChannelConfiguration load = WriteChannelConfiguration.newBuilder(tmpId)
                    .setFormatOptions(FormatOptions.json())
                    .setSchema(Schema.of(tmpFields))
                    .setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                    .setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
                    .build();
            TableDataWriteChannel writer = bq.writer(
                    JobId.newBuilder().setLocation(location).setRandomJob().build(), load);
            try (writer) {
                writer.write(ByteBuffer.wrap(json.toString().getBytes(StandardCharsets.UTF_8)));
            }
            Job loaded = writer.getJob().waitFor();
            if (loaded.getStatus().getError() != null) {
                throw new IllegalStateException("load failed: " + loaded.getStatus().getExecutionErrors());
            }

            String tmp = "`" + PROJECT + "." + DATASET + "." + tmpName + "`";
            List<String> cols = new ArrayList<>(repairable);
            String merge = "merge " + parsed + " p using " + tmp + " t on p.trade_id = t.trade_id "
                    + "when matched and (" + cols.stream()
                            .map(c -> "(p.`" + c + "` is null and t.`" + c + "` is not null)")
                            .collect(Collectors.joining(" or ")) + ") "
                    + "then update set " + cols.stream()
                            .map(c -> "`" + c + "` = coalesce(p.`" + c + "`, t.`" + c + "`)")
                            .collect(Collectors.joining(", "));
            Job merged = bq.create(JobInfo.of(JobId.newBuilder().setLocation(location).setRandomJob().build(),
                    QueryJobConfiguration.of(merge))).waitFor();
            if (merged.getStatus().getError() != null) {
                throw new IllegalStateException("merge failed: " + merged.getStatus().getError());
            }
            JobStatistics.QueryStatistics stats = merged.getStatistics();
            System.out.printf("repaired rows: %d%n", stats.getNumDmlAffectedRows());
        } finally {
            bq.delete(tmpId);
        }
    }

    private static String toJson(Object value) {
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        return SchemaRegistryOps.jsonString(value.toString());
    }
}
