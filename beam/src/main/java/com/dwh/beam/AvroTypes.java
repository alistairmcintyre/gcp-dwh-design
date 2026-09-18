package com.dwh.beam;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps an Avro field type onto the BigQuery column type it must become.
 *
 * <p>THE ONE THAT MATTERS IS DECIMAL. Avro has no decimal primitive: a decimal is {@code bytes} (or
 * {@code fixed}) carrying a {@code decimal} logical type with a precision and a scale. Ignore the
 * logical type and you map a price onto BigQuery {@code BYTES} -- which writes successfully and is
 * completely useless. Read the logical type but ignore the scale and you map scale-10 money onto
 * {@code NUMERIC}, whose scale is fixed at 9, and lose the last digit silently.
 *
 * <p>So the rule is: scale 9 or less fits {@code NUMERIC}; anything beyond needs {@code BIGNUMERIC},
 * which carries precision 76 and scale 38.
 */
public final class AvroTypes {

    /** BigQuery NUMERIC is fixed at precision 38, scale 9. */
    public static final int NUMERIC_MAX_PRECISION = 38;
    public static final int NUMERIC_MAX_SCALE = 9;

    private AvroTypes() {}

    /** The BigQuery type for every field on this record schema. */
    public static Map<String, BqType> columnTypes(Schema recordSchema) {
        Map<String, BqType> out = new LinkedHashMap<>();
        for (Schema.Field field : recordSchema.getFields()) {
            out.put(field.name(), toBigQuery(field.schema()));
        }
        return out;
    }

    public static BqType toBigQuery(Schema schema) {
        Schema resolved = unwrapNullable(schema);
        LogicalType logical = resolved.getLogicalType();

        if (logical instanceof LogicalTypes.Decimal) {
            LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logical;
            int precision = decimal.getPrecision();
            int scale = decimal.getScale();
            boolean fitsNumeric = precision <= NUMERIC_MAX_PRECISION && scale <= NUMERIC_MAX_SCALE;
            return fitsNumeric ? BqType.numeric(precision, scale) : BqType.bigNumeric(precision, scale);
        }
        if (logical instanceof LogicalTypes.TimestampMillis
                || logical instanceof LogicalTypes.TimestampMicros) {
            return BqType.TIMESTAMP;
        }
        if (logical instanceof LogicalTypes.Date) {
            return BqType.DATE;
        }

        switch (resolved.getType()) {
            case STRING:
            case ENUM:
                return BqType.STRING;
            case LONG:
            case INT:
                return BqType.INT64;
            case DOUBLE:
            case FLOAT:
                // Reachable, but a float in a money column is a rounding bug waiting to happen.
                // Producers should be emitting a decimal logical type instead.
                return BqType.FLOAT64;
            case BOOLEAN:
                return BqType.BOOL;
            case BYTES:
            case FIXED:
                return BqType.BYTES;
            default:
                throw new IllegalArgumentException(
                        "No BigQuery mapping for Avro type " + resolved.getType()
                                + " -- add one deliberately rather than letting the DDL step guess.");
        }
    }

    /** Avro models "optional" as a union with null; the real type is the other branch. */
    static Schema unwrapNullable(Schema schema) {
        if (schema.getType() != Schema.Type.UNION) {
            return schema;
        }
        for (Schema branch : schema.getTypes()) {
            if (branch.getType() != Schema.Type.NULL) {
                return branch;
            }
        }
        throw new IllegalArgumentException("Union contains only null: " + schema);
    }
}
