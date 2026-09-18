package com.dwh.beam;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Turns a {@link GenericRecord} into the column/value map BigQuery expects, by walking the record's
 * own schema rather than a list of known fields.
 *
 * <p>Walking the record is what makes a new field work without a redeploy: nothing in this class
 * names a column.
 *
 * <p>Decimals are the fiddly part. Avro encodes one as the unscaled value, two's complement,
 * big-endian, with the scale in the logical type. Pass those bytes straight through and BigQuery
 * stores meaningless binary; go via {@code double} and you get back the rounding error the producer
 * chose a decimal to avoid. Unscaled {@link BigInteger} plus scale into a {@link BigDecimal}, then
 * its plain string, which BigQuery parses into NUMERIC or BIGNUMERIC exactly.
 */
public class TableRowMapper implements Serializable {

    private static final long serialVersionUID = 1L;

    public Map<String, Object> toRow(GenericRecord record) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (Schema.Field field : record.getSchema().getFields()) {
            Object value = convert(field.schema(), record.get(field.name()));
            if (value != null) {
                row.put(field.name(), value);
            }
        }
        return row;
    }

    Object convert(Schema schema, Object value) {
        if (value == null) {
            return null;
        }
        Schema resolved = AvroTypes.unwrapNullable(schema);
        LogicalType logical = resolved.getLogicalType();

        if (logical instanceof LogicalTypes.Decimal) {
            int scale = ((LogicalTypes.Decimal) logical).getScale();
            return decimalToString(value, scale);
        }
        if (logical instanceof LogicalTypes.TimestampMillis) {
            return Instant.ofEpochMilli(((Number) value).longValue()).toString();
        }
        if (logical instanceof LogicalTypes.TimestampMicros) {
            long micros = ((Number) value).longValue();
            return Instant.ofEpochSecond(micros / 1_000_000L, (micros % 1_000_000L) * 1_000L).toString();
        }
        if (logical instanceof LogicalTypes.Date) {
            return LocalDate.ofEpochDay(((Number) value).longValue()).toString();
        }

        if (value instanceof Utf8) {
            return value.toString();
        }
        if (resolved.getType() == Schema.Type.ENUM) {
            return value.toString();
        }
        return value;
    }

    /** Unscaled big-endian two's complement plus scale, never via double. */
    static String decimalToString(Object value, int scale) {
        byte[] unscaled;
        if (value instanceof ByteBuffer) {
            ByteBuffer buffer = ((ByteBuffer) value).duplicate();
            unscaled = new byte[buffer.remaining()];
            buffer.get(unscaled);
        } else if (value instanceof byte[]) {
            unscaled = (byte[]) value;
        } else if (value instanceof BigDecimal) {
            return ((BigDecimal) value).toPlainString();
        } else {
            throw new IllegalArgumentException(
                    "Unexpected decimal encoding: " + value.getClass().getName());
        }
        return new BigDecimal(new BigInteger(unscaled), scale).toPlainString();
    }
}
