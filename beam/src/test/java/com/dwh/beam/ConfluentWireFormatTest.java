package com.dwh.beam;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConfluentWireFormatTest {

    private static byte[] framed(int schemaId, byte[] payload) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(ConfluentWireFormat.MAGIC_BYTE);
        out.write(ByteBuffer.allocate(4).putInt(schemaId).array());
        out.write(payload);
        return out.toByteArray();
    }

    @Test
    void readsTheSchemaIdFromTheFiveBytePrefix() throws Exception {
        byte[] message = framed(1234, new byte[] {1, 2, 3});
        assertEquals(1234, ConfluentWireFormat.schemaId(message));
    }

    @Test
    void stripsThePrefixFromThePayload() throws Exception {
        byte[] message = framed(7, new byte[] {9, 8, 7});
        assertArrayEquals(new byte[] {9, 8, 7}, ConfluentWireFormat.payload(message));
    }

    @Test
    void rejectsAMessageThatIsNotRegistryFramed() {
        // A plain Avro message has no magic byte. Decoding it as if it did would silently misread
        // the first five bytes of real data as a schema id, which is worse than failing.
        byte[] plainAvro = new byte[] {0x42, 0x01, 0x02, 0x03, 0x04, 0x05};
        assertThrows(IllegalArgumentException.class, () -> ConfluentWireFormat.schemaId(plainAvro));
    }

    @Test
    void rejectsAMessageTooShortToCarryAnId() {
        assertThrows(IllegalArgumentException.class,
                () -> ConfluentWireFormat.schemaId(new byte[] {0x0, 0x0}));
    }
}
