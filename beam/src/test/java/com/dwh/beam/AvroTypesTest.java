package com.dwh.beam;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The three cases that decide whether adding a field actually works.
 */
class AvroTypesTest {

    private static Schema decimal(int precision, int scale) {
        return LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Schema.Type.BYTES));
    }

    @Test
    void stringBecomesString() {
        assertEquals(BqType.STRING, AvroTypes.toBigQuery(Schema.create(Schema.Type.STRING)));
    }

    @Test
    void longBecomesInt64NotString() {
        // The failure this guards against is a DDL step that does not know the type and defaults to
        // STRING. BigQuery will never convert STRING to INT64, so that column is wrong for ever.
        assertEquals(BqType.INT64, AvroTypes.toBigQuery(Schema.create(Schema.Type.LONG)));
    }

    @Test
    void decimalWithinNineScaleBecomesNumeric() {
        assertEquals(BqType.numeric(18, 4), AvroTypes.toBigQuery(decimal(18, 4)));
        assertEquals("NUMERIC(18, 4)", AvroTypes.toBigQuery(decimal(18, 4)).ddl());
    }

    @Test
    void decimalBeyondNineScaleMustBecomeBigNumeric() {
        // BigQuery NUMERIC is fixed at scale 9. Mapping a scale-12 price onto NUMERIC silently
        // drops three digits -- in a money column, with no error. This is the one that matters.
        BqType mapped = AvroTypes.toBigQuery(decimal(30, 12));
        assertEquals("BIGNUMERIC", mapped.name());
        assertEquals(12, mapped.scale());
    }

    @Test
    void highPrecisionDecimalAlsoNeedsBigNumeric() {
        assertEquals("BIGNUMERIC", AvroTypes.toBigQuery(decimal(50, 2)).name());
    }

    @Test
    void nullableFieldsResolveToTheirUnderlyingType() {
        // A new field added compatibly is always a union with null. The null branch is not the type.
        Schema optionalLong = SchemaBuilder.unionOf().nullType().and().longType().endUnion();
        assertEquals(BqType.INT64, AvroTypes.toBigQuery(optionalLong));
    }

    @Test
    void timestampAndDateLogicalTypesSurvive() {
        Schema ts = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
        Schema date = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
        assertEquals(BqType.TIMESTAMP, AvroTypes.toBigQuery(ts));
        assertEquals(BqType.DATE, AvroTypes.toBigQuery(date));
        // Without the logical type these would be INT64 -- a timestamp stored as a bare number.
        assertEquals(BqType.INT64, AvroTypes.toBigQuery(Schema.create(Schema.Type.LONG)));
    }

    @Test
    void mapsEveryFieldOnARecord() {
        Schema record = SchemaBuilder.record("Trade").fields()
                .requiredString("trade_id")
                .requiredLong("quantity")
                .name("price").type(decimal(18, 9)).noDefault()
                .endRecord();

        Map<String, BqType> columns = AvroTypes.columnTypes(record);
        assertEquals(BqType.STRING, columns.get("trade_id"));
        assertEquals(BqType.INT64, columns.get("quantity"));
        assertEquals(BqType.numeric(18, 9), columns.get("price"));
    }

    @Test
    void bigQueryWideningRulesAreRespected() {
        assertTrue(BqType.numeric(38, 9).canAccept(BqType.INT64));       // INT64 -> NUMERIC, allowed
        assertTrue(BqType.bigNumeric(76, 38).canAccept(BqType.numeric(18, 4)));
        assertTrue(BqType.FLOAT64.canAccept(BqType.INT64));

        // Nothing converts to or from STRING. A column guessed as STRING is permanently wrong.
        assertFalse(BqType.STRING.canAccept(BqType.INT64));
        assertFalse(BqType.INT64.canAccept(BqType.STRING));
        // And narrowing is never safe: scale 9 cannot hold scale 12.
        assertFalse(BqType.numeric(38, 9).canAccept(BqType.bigNumeric(50, 12)));
    }
}
