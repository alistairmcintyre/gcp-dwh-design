package com.dwh.beam;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The question this answers: a producer registers v2 with one extra field and starts sending it.
 * Does the value reach BigQuery without redeploying the pipeline?
 */
class AvroDecodeTest {

    private static final Schema V1 = SchemaBuilder.record("Trade").namespace("trading").fields()
            .requiredString("trade_id")
            .requiredLong("quantity")
            .endRecord();

    private static Schema decimalType(int precision, int scale) {
        return LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Schema.Type.BYTES));
    }

    /** v2 adds one optional field of the given type -- the shape of a BACKWARD-compatible change. */
    private static Schema v2With(String fieldName, Schema fieldType) {
        return SchemaBuilder.record("Trade").namespace("trading").fields()
                .requiredString("trade_id")
                .requiredLong("quantity")
                .name(fieldName).type().unionOf().nullType().and().type(fieldType).endUnion().noDefault()
                .endRecord();
    }

    private static byte[] framed(int schemaId, GenericRecord record) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(ConfluentWireFormat.MAGIC_BYTE);
        out.write(ByteBuffer.allocate(4).putInt(schemaId).array());
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    /** A resolver standing in for the registry: ids the pipeline has never seen still resolve. */
    private static AvroSchemaResolver resolverOf(Map<Integer, Schema> schemas) {
        Map<Integer, Schema> copy = new HashMap<>(schemas);
        return id -> {
            Schema s = copy.get(id);
            if (s == null) {
                throw new IllegalStateException("Unknown schema id " + id);
            }
            return s;
        };
    }

    private Map<String, Object> decodeOne(int schemaId, GenericRecord record, Map<Integer, Schema> registry)
            throws Exception {
        GenericRecordDecoder decoder = new GenericRecordDecoder(resolverOf(registry));
        return new TableRowMapper().toRow(decoder.decode(framed(schemaId, record)));
    }

    @Test
    void newStringFieldArrivesWithNoRedeploy() throws Exception {
        Schema v2 = v2With("venue", Schema.create(Schema.Type.STRING));
        GenericRecord r = new GenericData.Record(v2);
        r.put("trade_id", "T1");
        r.put("quantity", 10L);
        r.put("venue", "LSE");

        Map<String, Object> row = decodeOne(2, r, Map.of(1, V1, 2, v2));
        assertEquals("LSE", row.get("venue"));
    }

    @Test
    void newLongFieldArrivesAsANumberNotAString() throws Exception {
        Schema v2 = v2With("settlement_days", Schema.create(Schema.Type.LONG));
        GenericRecord r = new GenericData.Record(v2);
        r.put("trade_id", "T2");
        r.put("quantity", 10L);
        r.put("settlement_days", 3L);

        Map<String, Object> row = decodeOne(3, r, Map.of(1, V1, 3, v2));
        assertEquals(3L, row.get("settlement_days"));
        assertFalse(row.get("settlement_days") instanceof String,
                "a long must not arrive as text -- BigQuery cannot widen STRING back to INT64");
    }

    @Test
    void newDecimalFieldKeepsEveryDigit() throws Exception {
        // 1234567.891234 at scale 6 -- the kind of price a double cannot hold exactly.
        BigDecimal price = new BigDecimal("1234567.891234");
        Schema decimal = decimalType(20, 6);
        Schema v2 = v2With("price", decimal);

        GenericRecord r = new GenericData.Record(v2);
        r.put("trade_id", "T3");
        r.put("quantity", 10L);
        r.put("price", ByteBuffer.wrap(price.unscaledValue().toByteArray()));

        Map<String, Object> row = decodeOne(4, r, Map.of(1, V1, 4, v2));
        assertEquals("1234567.891234", row.get("price"));
        assertEquals(0, new BigDecimal((String) row.get("price")).compareTo(price));
    }

    @Test
    void decimalRoundTripsExactlyWhereADoubleWouldNot() {
        // 0.1 + 0.2 in binary floating point is famously not 0.3. The decimal path must not care.
        BigDecimal exact = new BigDecimal("0.30000000000000004");
        byte[] unscaled = exact.unscaledValue().toByteArray();
        String result = TableRowMapper.decimalToString(ByteBuffer.wrap(unscaled), exact.scale());
        assertEquals("0.30000000000000004", result);
        assertTrue(new BigDecimal(result).compareTo(exact) == 0);
    }

    @Test
    void negativeDecimalsSurviveTwosComplement() {
        BigDecimal negative = new BigDecimal("-450.25");
        String result = TableRowMapper.decimalToString(
                ByteBuffer.wrap(negative.unscaledValue().toByteArray()), negative.scale());
        assertEquals("-450.25", result);
    }

    @Test
    void oldMessagesStillDecodeAfterTheSchemaEvolves() throws Exception {
        // In-flight v1 messages keep arriving while v2 rolls out. Both must work at once, which is
        // what resolving per message by schema id buys and a pinned reader schema would lose.
        GenericRecord old = new GenericData.Record(V1);
        old.put("trade_id", "T0");
        old.put("quantity", 5L);

        Schema v2 = v2With("venue", Schema.create(Schema.Type.STRING));
        Map<String, Object> row = decodeOne(1, old, Map.of(1, V1, 2, v2));

        assertEquals("T0", row.get("trade_id"));
        assertFalse(row.containsKey("venue"), "a v1 message has no v2 field and must not invent one");
    }

    @Test
    void unscaledBigIntegerMathIsUsedRatherThanDouble() {
        // A value with more significant digits than a double can represent.
        BigInteger unscaled = new BigInteger("123456789012345678901234567890");
        String result = TableRowMapper.decimalToString(ByteBuffer.wrap(unscaled.toByteArray()), 10);
        assertEquals("12345678901234567890.1234567890", result);
    }
}
