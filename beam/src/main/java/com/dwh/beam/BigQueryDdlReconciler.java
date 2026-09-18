package com.dwh.beam;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The DDL step: bring a BigQuery table up to date with a registered Avro schema.
 *
 * <p>There is no BigQuery endpoint that takes a schema and works out the difference for you. It is
 * a read and a write: {@code getTable} gives you the current schema, you diff it yourself, and
 * {@code table.toBuilder().setDefinition(...).build().update()} writes a complete schema back. Send
 * only the new columns and you have asked BigQuery to drop every column you left out. It refuses,
 * but the call is not a patch.
 *
 * <p>What it accepts on an existing table is narrow: a new NULLABLE column, REQUIRED relaxed to
 * NULLABLE, and a few widenings (INT64 to NUMERIC, BIGNUMERIC or FLOAT64; NUMERIC to BIGNUMERIC or
 * FLOAT64). Dropping a column, renaming one or narrowing a type are all rejected.
 *
 * <p>Run this before the producer ships, from its CI or from a listener on the registry. Here only
 * the load tests call it, straight after registering a version. Adding the column early is
 * necessary but not sufficient: a running Storage Write API pipeline only picks it up with
 * {@code withAutoSchemaUpdate}, and only once BigQuery's reply to a write says the schema changed
 * (arm A in the README).
 */
public class BigQueryDdlReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryDdlReconciler.class);

    private final BigQuery bigquery;

    public BigQueryDdlReconciler(BigQuery bigquery) {
        this.bigquery = bigquery;
    }

    /**
     * Add any column the Avro schema has that the table does not.
     *
     * @return the columns that were added, empty if the table was already up to date
     */
    public Map<String, BqType> reconcile(TableId tableId, org.apache.avro.Schema avroSchema) {
        Table table = bigquery.getTable(tableId);
        if (table == null) {
            throw new IllegalStateException("Table does not exist: " + tableId
                    + " -- creating it is a separate, deliberate act, not a side effect of ingestion.");
        }

        Schema current = table.getDefinition().getSchema();
        if (current == null) {
            throw new IllegalStateException("Table has no schema: " + tableId);
        }

        Map<String, Field> existing = new LinkedHashMap<>();
        current.getFields().forEach(f -> existing.put(f.getName(), f));

        Map<String, BqType> wanted = AvroTypes.columnTypes(avroSchema);
        Map<String, BqType> toAdd = new LinkedHashMap<>();
        wanted.forEach((name, type) -> {
            if (!existing.containsKey(name)) {
                toAdd.put(name, type);
            }
        });

        if (toAdd.isEmpty()) {
            LOG.debug("{} is already up to date", tableId);
            return toAdd;
        }

        // Send the whole schema back: existing fields first, then the additions.
        List<Field> merged = new ArrayList<>(current.getFields());
        toAdd.forEach((name, type) -> merged.add(toField(name, type)));

        LOG.info("Adding {} to {}", toAdd, tableId);
        bigquery.update(
                table.toBuilder()
                        .setDefinition(StandardTableDefinition.of(Schema.of(merged)))
                        .build());
        return toAdd;
    }

    /** Always NULLABLE: it is the only addition BigQuery permits, and existing rows have no value. */
    static Field toField(String name, BqType type) {
        Field.Builder builder;
        switch (type.name()) {
            case "NUMERIC":
            case "BIGNUMERIC":
                // Precision and scale are carried through deliberately. A decimal column created
                // without them takes BigQuery's defaults, and a scale-12 price silently loses
                // three digits against NUMERIC's fixed scale of 9.
                builder = Field.newBuilder(name, StandardSQLTypeName.valueOf(type.name()))
                        .setPrecision((long) type.precision())
                        .setScale((long) type.scale());
                break;
            default:
                builder = Field.newBuilder(name, LegacySQLTypeName.valueOfStrict(legacyName(type.name())));
        }
        return builder.setMode(Field.Mode.NULLABLE).build();
    }

    private static String legacyName(String standardName) {
        switch (standardName) {
            case "INT64":   return "INTEGER";
            case "FLOAT64": return "FLOAT";
            case "BOOL":    return "BOOLEAN";
            default:        return standardName;
        }
    }
}
