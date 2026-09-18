package com.dwh.beam;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Confluent's Avro wire format.
 *
 * <p>Every message a Confluent serialiser produces is laid out as:
 *
 * <pre>
 *   byte 0      magic byte, always 0x00
 *   bytes 1-4   schema id, 4-byte big-endian int
 *   bytes 5..   the Avro-encoded payload
 * </pre>
 *
 * <p>The five-byte prefix is the reason a plain Avro decode of a Kafka message fails: the decoder
 * sees the magic byte and the id as payload. It is also what makes this whole approach possible --
 * the id tells us exactly which registered schema wrote this row, before we decode anything.
 */
public final class ConfluentWireFormat {

    public static final byte MAGIC_BYTE = 0x0;
    public static final int PREFIX_LENGTH = 5;

    private ConfluentWireFormat() {}

    /** Returns the schema id, or throws if this is not a Confluent-framed message. */
    public static int schemaId(byte[] message) {
        if (message == null || message.length < PREFIX_LENGTH) {
            throw new IllegalArgumentException(
                    "Message is too short to carry a Confluent schema id: "
                            + (message == null ? "null" : message.length + " bytes"));
        }
        if (message[0] != MAGIC_BYTE) {
            throw new IllegalArgumentException(
                    "Expected Confluent magic byte 0x00 but found 0x"
                            + Integer.toHexString(message[0] & 0xff)
                            + " -- is this topic actually Avro from the registry?");
        }
        return ByteBuffer.wrap(message, 1, 4).getInt();
    }

    /** What a Confluent serialiser produces: prefix, then the record in Avro binary. */
    public static byte[] frame(int schemaId, GenericRecord record) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(MAGIC_BYTE);
        out.write(ByteBuffer.allocate(4).putInt(schemaId).array());
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    /** The Avro payload with the five-byte prefix removed. */
    public static byte[] payload(byte[] message) {
        int length = message.length - PREFIX_LENGTH;
        byte[] out = new byte[length];
        System.arraycopy(message, PREFIX_LENGTH, out, 0, length);
        return out;
    }
}
